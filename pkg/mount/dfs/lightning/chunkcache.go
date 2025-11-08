package lightning

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/singleflight"
)

// ChunkCache implements a chunk-based cache for random access patterns
// Uses 4MB chunks with memory LRU and disk backing
type ChunkCache struct {
	// Configuration
	chunkSize    int64
	memCacheSize int64 // Total memory for cache
	downloader   Downloader

	// Memory cache (LRU)
	memCache   *LRUCache
	memCacheMu sync.RWMutex

	// Disk cache (sparse file via shadow writer)
	shadowWriter *ShadowWriter

	// Range tracking
	tracker *RangeTracker

	// Statistics
	stats *ChunkCacheStats

	// Connection pool for downloads
	connPool *ConnectionPool

	// Singleflight for deduplicating concurrent downloads
	downloadGroup singleflight.Group
}

// Downloader is a function that downloads a byte range
type Downloader func(ctx context.Context, offset, size int64) ([]byte, error)

// ChunkCacheStats tracks cache performance
type ChunkCacheStats struct {
	MemHits       atomic.Int64
	DiskHits      atomic.Int64
	Misses        atomic.Int64
	BytesServed   atomic.Int64
	ChunksLoaded  atomic.Int64
	ChunksEvicted atomic.Int64
}

// LRUCache is a simple LRU cache for chunks
type LRUCache struct {
	capacity int64 // In bytes
	size     atomic.Int64
	items    map[string]*cacheItem
	lruList  *lruList
	mu       sync.RWMutex
}

// cacheItem represents a cached chunk
type cacheItem struct {
	key     string
	chunk   *Chunk
	size    int64
	lruNode *lruNode
}

// Chunk represents a cached data chunk
type Chunk struct {
	fileID string
	offset int64 // Chunk-aligned offset
	size   int64
	data   []byte
}

// lruList is a doubly-linked list for LRU
type lruList struct {
	head *lruNode
	tail *lruNode
	mu   sync.Mutex
}

type lruNode struct {
	key  string
	prev *lruNode
	next *lruNode
}

// RangeTracker tracks which byte ranges are cached
type RangeTracker struct {
	mu     sync.RWMutex
	ranges map[string]*rangeSet // fileID -> ranges
}

// rangeSet tracks cached ranges for a file
type rangeSet struct {
	intervals []interval
}

// interval represents a contiguous cached range
type interval struct {
	start int64
	end   int64
}

const (
	defaultChunkSize    = 4 * 1024 * 1024   // 4MB
	defaultMemCacheSize = 256 * 1024 * 1024 // 256MB
)

// NewChunkCache creates a new chunk cache
func NewChunkCache(connPool *ConnectionPool, shadowWriter *ShadowWriter, memCacheSize int64) *ChunkCache {
	if memCacheSize <= 0 {
		memCacheSize = defaultMemCacheSize
	}

	cc := &ChunkCache{
		chunkSize:    defaultChunkSize,
		memCacheSize: memCacheSize,
		connPool:     connPool,
		shadowWriter: shadowWriter,
		memCache:     newLRUCache(memCacheSize),
		tracker:      newRangeTracker(),
		stats:        &ChunkCacheStats{},
	}

	return cc
}

// ReadAt reads data from cache, downloading if necessary
func (cc *ChunkCache) ReadAt(ctx context.Context, fileID string, p []byte, offset int64) (int, error) {
	size := int64(len(p))
	if size == 0 {
		return 0, nil
	}

	// Determine which chunks are needed
	firstChunk := offset / cc.chunkSize
	lastChunk := (offset + size - 1) / cc.chunkSize

	bytesRead := 0
	currentOffset := offset

	// Read from each chunk
	for chunkNum := firstChunk; chunkNum <= lastChunk; chunkNum++ {
		chunkOffset := chunkNum * cc.chunkSize

		// Get chunk
		chunk, err := cc.getChunk(ctx, fileID, chunkOffset)
		if err != nil {
			if bytesRead > 0 {
				return bytesRead, nil // Return partial read
			}
			return 0, err
		}

		// Calculate read position within chunk
		offsetInChunk := currentOffset - chunkOffset
		remainingInChunk := chunk.size - offsetInChunk
		toRead := size - int64(bytesRead)

		if toRead > remainingInChunk {
			toRead = remainingInChunk
		}

		// Copy data
		copy(p[bytesRead:], chunk.data[offsetInChunk:offsetInChunk+toRead])
		bytesRead += int(toRead)
		currentOffset += toRead

		if bytesRead >= int(size) {
			break
		}
	}

	cc.stats.BytesServed.Add(int64(bytesRead))
	return bytesRead, nil
}

// getChunk retrieves a chunk from cache or downloads it
func (cc *ChunkCache) getChunk(ctx context.Context, fileID string, chunkOffset int64) (*Chunk, error) {
	key := chunkKey(fileID, chunkOffset)

	// Try memory cache first
	if chunk := cc.memCache.get(key); chunk != nil {
		cc.stats.MemHits.Add(1)
		return chunk, nil
	}

	// Use singleflight for downloads (deduplicate concurrent requests)
	v, err, shared := cc.downloadGroup.Do(key, func() (interface{}, error) {
		// Double-check memory cache after acquiring singleflight lock
		if chunk := cc.memCache.get(key); chunk != nil {
			cc.stats.MemHits.Add(1)
			return chunk, nil
		}

		// Try disk cache
		if cc.shadowWriter != nil {
			if cc.shadowWriter.HasCachedData(fileID, chunkOffset, cc.chunkSize) {
				data, err := cc.shadowWriter.ReadCachedData(fileID, chunkOffset, cc.chunkSize)
				if err == nil {
					chunk := &Chunk{
						fileID: fileID,
						offset: chunkOffset,
						size:   int64(len(data)),
						data:   data,
					}
					// Add to memory cache
					cc.memCache.put(key, chunk)
					cc.stats.DiskHits.Add(1)
					return chunk, nil
				}
			}
		}

		// Cache miss - download (only ONE goroutine does this)
		cc.stats.Misses.Add(1)
		return cc.downloadChunk(ctx, fileID, chunkOffset)
	})

	if err != nil {
		return nil, err
	}

	chunk := v.(*Chunk)

	if shared {
		// We waited for another goroutine's download - count as memory hit (saved bandwidth)
		cc.stats.MemHits.Add(1)
	}

	return chunk, nil
}

// downloadChunk downloads a chunk from the remote source
func (cc *ChunkCache) downloadChunk(ctx context.Context, fileID string, chunkOffset int64) (*Chunk, error) {
	// Download via connection pool
	reader, err := cc.connPool.Download(ctx, chunkOffset, cc.chunkSize)
	if err != nil {
		return nil, fmt.Errorf("download chunk: %w", err)
	}
	defer func(reader io.ReadCloser) {
		_ = reader.Close()
	}(reader)

	// Read all data
	data := make([]byte, cc.chunkSize)
	n, err := io.ReadFull(reader, data)
	if err != nil && err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, fmt.Errorf("read chunk data: %w", err)
	}

	data = data[:n]

	chunk := &Chunk{
		fileID: fileID,
		offset: chunkOffset,
		size:   int64(n),
		data:   data,
	}

	// Add to memory cache
	key := chunkKey(fileID, chunkOffset)
	cc.memCache.put(key, chunk)
	cc.stats.ChunksLoaded.Add(1)

	// Queue shadow write to disk
	if cc.shadowWriter != nil {
		_ = cc.shadowWriter.QueueWrite(fileID, chunkOffset, data)
	}

	// Track range
	cc.tracker.addRange(fileID, chunkOffset, chunkOffset+int64(n))

	return chunk, nil
}

// chunkKey generates cache key for a chunk (allocation-free)
func chunkKey(fileID string, offset int64) string {
	// Pre-allocate buffer to avoid allocations
	buf := make([]byte, 0, len(fileID)+20)
	buf = append(buf, fileID...)
	buf = append(buf, ':')
	// Use optimized integer conversion
	if offset == 0 {
		buf = append(buf, '0')
	} else {
		// Manual conversion is faster than fmt.Sprintf and strconv for common cases
		if offset < 0 {
			buf = append(buf, '-')
			offset = -offset
		}
		// Build number backwards
		start := len(buf)
		for offset > 0 {
			buf = append(buf, byte('0'+offset%10))
			offset /= 10
		}
		// Reverse the digits
		end := len(buf) - 1
		for i := start; i < start+(end-start+1)/2; i++ {
			buf[i], buf[end-(i-start)] = buf[end-(i-start)], buf[i]
		}
	}
	return string(buf)
}

// newLRUCache creates a new LRU cache
func newLRUCache(capacity int64) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		items:    make(map[string]*cacheItem),
		lruList:  newLRUList(),
	}
}

// get retrieves a chunk from cache
func (lru *LRUCache) get(key string) *Chunk {
	lru.mu.RLock()
	item, exists := lru.items[key]
	lru.mu.RUnlock()

	if !exists {
		return nil
	}

	// Move to front (most recently used)
	lru.lruList.moveToFront(item.lruNode)

	return item.chunk
}

// put adds a chunk to cache
func (lru *LRUCache) put(key string, chunk *Chunk) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	// Check if already exists
	if item, exists := lru.items[key]; exists {
		// Update and move to front
		item.chunk = chunk
		item.size = int64(len(chunk.data))
		lru.lruList.moveToFront(item.lruNode)
		return
	}

	// Evict if necessary
	chunkSize := int64(len(chunk.data))
	for lru.size.Load()+chunkSize > lru.capacity && lru.lruList.tail != nil {
		lru.evictLRU()
	}

	// Add new item
	node := lru.lruList.addToFront(key)
	item := &cacheItem{
		key:     key,
		chunk:   chunk,
		size:    chunkSize,
		lruNode: node,
	}

	lru.items[key] = item
	lru.size.Add(chunkSize)
}

// evictLRU removes the least recently used item
func (lru *LRUCache) evictLRU() {
	if lru.lruList.tail == nil {
		return
	}

	key := lru.lruList.tail.key
	if item, exists := lru.items[key]; exists {
		delete(lru.items, key)
		lru.size.Add(-item.size)
		lru.lruList.remove(item.lruNode)
	}
}

// newLRUList creates a new LRU list
func newLRUList() *lruList {
	return &lruList{}
}

// addToFront adds a node to the front of the list
func (ll *lruList) addToFront(key string) *lruNode {
	ll.mu.Lock()
	defer ll.mu.Unlock()

	node := &lruNode{key: key}

	if ll.head == nil {
		ll.head = node
		ll.tail = node
	} else {
		node.next = ll.head
		ll.head.prev = node
		ll.head = node
	}

	return node
}

// moveToFront moves a node to the front
func (ll *lruList) moveToFront(node *lruNode) {
	if node == nil || node == ll.head {
		return
	}

	ll.mu.Lock()
	defer ll.mu.Unlock()

	// Remove from current position
	if node.prev != nil {
		node.prev.next = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	}
	if node == ll.tail {
		ll.tail = node.prev
	}

	// Add to front
	node.prev = nil
	node.next = ll.head
	if ll.head != nil {
		ll.head.prev = node
	}
	ll.head = node
}

// remove removes a node from the list
func (ll *lruList) remove(node *lruNode) {
	if node == nil {
		return
	}

	ll.mu.Lock()
	defer ll.mu.Unlock()

	if node.prev != nil {
		node.prev.next = node.next
	} else {
		ll.head = node.next
	}

	if node.next != nil {
		node.next.prev = node.prev
	} else {
		ll.tail = node.prev
	}
}

// newRangeTracker creates a new range tracker
func newRangeTracker() *RangeTracker {
	return &RangeTracker{
		ranges: make(map[string]*rangeSet),
	}
}

// addRange adds a cached range
func (rt *RangeTracker) addRange(fileID string, start, end int64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rs, exists := rt.ranges[fileID]
	if !exists {
		rs = &rangeSet{
			intervals: make([]interval, 0),
		}
		rt.ranges[fileID] = rs
	}

	// Add interval and merge overlapping
	rs.add(start, end)
}

// hasRange checks if a range is fully cached
func (rt *RangeTracker) hasRange(fileID string, start, end int64) bool {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	rs, exists := rt.ranges[fileID]
	if !exists {
		return false
	}

	return rs.contains(start, end)
}

// add adds an interval to the range set
func (rs *rangeSet) add(start, end int64) {
	newInterval := interval{start: start, end: end}

	// Find position and merge overlapping intervals
	merged := make([]interval, 0, len(rs.intervals)+1)
	added := false

	for _, existing := range rs.intervals {
		if newInterval.end < existing.start {
			// New interval comes before existing
			if !added {
				merged = append(merged, newInterval)
				added = true
			}
			merged = append(merged, existing)
		} else if newInterval.start > existing.end {
			// New interval comes after existing
			merged = append(merged, existing)
		} else {
			// Overlapping - merge
			if newInterval.start > existing.start {
				newInterval.start = existing.start
			}
			if newInterval.end < existing.end {
				newInterval.end = existing.end
			}
		}
	}

	if !added {
		merged = append(merged, newInterval)
	}

	rs.intervals = merged
}

// contains checks if a range is fully contained
func (rs *rangeSet) contains(start, end int64) bool {
	for _, interval := range rs.intervals {
		if start >= interval.start && end <= interval.end {
			return true
		}
	}
	return false
}

// Stats returns cache statistics
func (cc *ChunkCache) Stats() *ChunkCacheStats {
	return cc.stats
}

// Clear clears the cache
func (cc *ChunkCache) Clear() {
	cc.memCacheMu.Lock()
	defer cc.memCacheMu.Unlock()

	cc.memCache = newLRUCache(cc.memCacheSize)
	cc.tracker = newRangeTracker()
}
