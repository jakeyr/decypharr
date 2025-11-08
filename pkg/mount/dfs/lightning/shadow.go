package lightning

import (
	"container/list"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// ShadowWriter performs asynchronous background cache writes
// during hot path streaming without blocking the read path
type ShadowWriter struct {
	// Configuration
	cacheDir    string
	workerCount int
	queueSize   int

	// Workers
	queue   chan *WriteOperation
	workers []*shadowWorker
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	// Statistics
	stats *ShadowStats

	// State
	started atomic.Bool
	closed  atomic.Bool
}

// WriteOperation represents a pending cache write
type WriteOperation struct {
	fileID   string
	offset   int64
	data     []byte
	priority int
	retries  int
}

// ShadowStats tracks shadow writer performance
type ShadowStats struct {
	BytesWritten    atomic.Int64
	WritesCompleted atomic.Int64
	WritesFailed    atomic.Int64
	WritesQueued    atomic.Int64
	WritesDropped   atomic.Int64 // Dropped due to full queue
	AverageLatency  atomic.Int64 // Microseconds
	QueueDepth      atomic.Int32
}

// cachedFile represents a cached file handle with LRU tracking
type cachedFile struct {
	file    *os.File
	lastUse time.Time
	element *list.Element
}

// shadowWorker is a background worker that processes writes
type shadowWorker struct {
	id            int
	sw            *ShadowWriter
	ctx           context.Context
	fileCache     map[string]*cachedFile // Open file cache with LRU
	fileCacheLRU  *list.List             // LRU queue
	openFileCount int
	mu            sync.Mutex
}

const (
	defaultWorkerCount = 4
	defaultQueueSize   = 256 // 256 pending operations
	maxRetries         = 3
	writeTimeout       = 30 * time.Second
	maxOpenFiles       = 32 // Maximum open file descriptors per worker

	// File open flags
	cacheFileFlags = os.O_CREATE | os.O_RDWR
	cacheFileMode  = 0644
)

// NewShadowWriter creates a new shadow cache writer
func NewShadowWriter(ctx context.Context, cacheDir string) (*ShadowWriter, error) {
	if cacheDir == "" {
		return nil, fmt.Errorf("cache directory required")
	}

	// Ensure cache directory exists
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return nil, fmt.Errorf("create cache directory: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)

	sw := &ShadowWriter{
		cacheDir:    cacheDir,
		workerCount: defaultWorkerCount,
		queueSize:   defaultQueueSize,
		queue:       make(chan *WriteOperation, defaultQueueSize),
		workers:     make([]*shadowWorker, defaultWorkerCount),
		ctx:         ctx,
		cancel:      cancel,
		stats:       &ShadowStats{},
	}

	return sw, nil
}

// Start starts the shadow writer workers
func (sw *ShadowWriter) Start() error {
	if sw.started.CompareAndSwap(false, true) {
		// Create workers
		for i := 0; i < sw.workerCount; i++ {
			worker := &shadowWorker{
				id:           i,
				sw:           sw,
				ctx:          sw.ctx,
				fileCache:    make(map[string]*cachedFile),
				fileCacheLRU: list.New(),
			}
			sw.workers[i] = worker

			// Start worker
			sw.wg.Add(1)
			go worker.run()
		}
	}
	return nil
}

// QueueWrite queues a cache write operation (non-blocking)
func (sw *ShadowWriter) QueueWrite(fileID string, offset int64, data []byte) error {
	if sw.closed.Load() {
		return fmt.Errorf("shadow writer is closed")
	}

	if !sw.started.Load() {
		return fmt.Errorf("shadow writer not started")
	}

	// Create write operation
	op := &WriteOperation{
		fileID:   fileID,
		offset:   offset,
		data:     data,
		priority: 0,
	}

	// Try to queue (non-blocking)
	select {
	case sw.queue <- op:
		sw.stats.WritesQueued.Add(1)
		sw.stats.QueueDepth.Add(1)
		return nil
	default:
		// Queue full - drop write
		sw.stats.WritesDropped.Add(1)
		return fmt.Errorf("shadow write queue full")
	}
}

// QueueWriteBlocking queues a write with blocking
func (sw *ShadowWriter) QueueWriteBlocking(ctx context.Context, fileID string, offset int64, data []byte) error {
	if sw.closed.Load() {
		return fmt.Errorf("shadow writer is closed")
	}

	op := &WriteOperation{
		fileID: fileID,
		offset: offset,
		data:   data,
	}

	select {
	case sw.queue <- op:
		sw.stats.WritesQueued.Add(1)
		sw.stats.QueueDepth.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-sw.ctx.Done():
		return fmt.Errorf("shadow writer stopped")
	}
}

// run is the worker main loop
func (w *shadowWorker) run() {
	defer w.sw.wg.Done()
	defer w.closeAllFiles()

	for {
		select {
		case op := <-w.sw.queue:
			w.sw.stats.QueueDepth.Add(-1)
			w.processWrite(op)

		case <-w.ctx.Done():
			return
		}
	}
}

// processWrite processes a single write operation
func (w *shadowWorker) processWrite(op *WriteOperation) {
	startTime := time.Now()

	// Get or open cache file
	file, err := w.getCacheFile(op.fileID)
	if err != nil {
		w.sw.stats.WritesFailed.Add(1)
		w.retryOrFail(op, err)
		return
	}

	// Write data at offset
	_, cancel := context.WithTimeout(w.ctx, writeTimeout)
	defer cancel()

	// Use pwrite to write at specific offset
	n, err := file.WriteAt(op.data, op.offset)
	if err != nil {
		w.sw.stats.WritesFailed.Add(1)
		w.retryOrFail(op, err)
		return
	}

	// Sync periodically (every 10th write)
	if w.sw.stats.WritesCompleted.Load()%10 == 0 {
		_ = file.Sync()
	}

	// Update stats
	latency := time.Since(startTime).Microseconds()
	w.sw.stats.BytesWritten.Add(int64(n))
	w.sw.stats.WritesCompleted.Add(1)
	w.updateAverageLatency(latency)
}

// getCacheFile gets or opens a cache file with LRU eviction
func (w *shadowWorker) getCacheFile(fileID string) (*os.File, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Check cache
	if cf, exists := w.fileCache[fileID]; exists {
		// Update LRU - move to front
		cf.lastUse = time.Now()
		w.fileCacheLRU.MoveToFront(cf.element)
		return cf.file, nil
	}

	// Evict if at limit
	if w.openFileCount >= maxOpenFiles {
		w.evictLRU()
	}

	// Open file
	cachePath := w.getCachePath(fileID)

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(cachePath), 0755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

	file, err := os.OpenFile(cachePath, cacheFileFlags, cacheFileMode)
	if err != nil {
		return nil, fmt.Errorf("open cache file: %w", err)
	}

	// Cache file handle with LRU tracking
	cf := &cachedFile{
		file:    file,
		lastUse: time.Now(),
		element: w.fileCacheLRU.PushFront(fileID),
	}
	w.fileCache[fileID] = cf
	w.openFileCount++

	return file, nil
}

// evictLRU removes the least recently used file from cache
func (w *shadowWorker) evictLRU() {
	if w.fileCacheLRU.Len() == 0 {
		return
	}

	// Get oldest element from back of list
	elem := w.fileCacheLRU.Back()
	if elem == nil {
		return
	}

	fileID := elem.Value.(string)

	if cf, exists := w.fileCache[fileID]; exists {
		// Sync and close file
		_ = cf.file.Sync()
		_ = cf.file.Close()

		// Remove from cache
		delete(w.fileCache, fileID)
		w.fileCacheLRU.Remove(elem)
		w.openFileCount--
	}
}

// getCachePath returns the cache file path for a file ID
func (w *shadowWorker) getCachePath(fileID string) string {
	// Use first 2 chars as subdirectory for better distribution
	subdir := ""
	if len(fileID) >= 2 {
		subdir = fileID[:2]
	}

	return filepath.Join(w.sw.cacheDir, subdir, fileID+".cache")
}

// retryOrFail retries a failed write or logs failure
func (w *shadowWorker) retryOrFail(op *WriteOperation, err error) {
	op.retries++

	if op.retries < maxRetries {
		// Retry
		select {
		case w.sw.queue <- op:
			// Queued for retry
		default:
			// Queue full, give up
			w.sw.stats.WritesFailed.Add(1)
		}
	} else {
		// Max retries exceeded
		w.sw.stats.WritesFailed.Add(1)
	}
}

// updateAverageLatency updates the moving average latency
func (w *shadowWorker) updateAverageLatency(newLatency int64) {
	oldAvg := w.sw.stats.AverageLatency.Load()
	if oldAvg == 0 {
		w.sw.stats.AverageLatency.Store(newLatency)
		return
	}

	// Exponential moving average
	newAvg := int64(float64(oldAvg)*0.9 + float64(newLatency)*0.1)
	w.sw.stats.AverageLatency.Store(newAvg)
}

// closeAllFiles closes all cached file handles
func (w *shadowWorker) closeAllFiles() {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, cf := range w.fileCache {
		if cf != nil && cf.file != nil {
			_ = cf.file.Sync()
			_ = cf.file.Close()
		}
	}
	w.fileCache = make(map[string]*cachedFile)
	w.fileCacheLRU = list.New()
	w.openFileCount = 0
}

// Close stops the shadow writer and waits for pending writes
func (sw *ShadowWriter) Close() error {
	if sw.closed.CompareAndSwap(false, true) {
		// Stop accepting new writes
		close(sw.queue)

		// Cancel workers
		sw.cancel()

		// Wait for workers to finish
		sw.wg.Wait()
	}
	return nil
}

// Flush waits for all queued writes to complete
func (sw *ShadowWriter) Flush(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for {
		queueDepth := sw.stats.QueueDepth.Load()
		if queueDepth == 0 {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("flush timeout: %d writes pending", queueDepth)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// Stats returns shadow writer statistics
func (sw *ShadowWriter) Stats() *ShadowStats {
	return sw.stats
}

// GetCachePath returns the cache file path for a file
func (sw *ShadowWriter) GetCachePath(fileID string) string {
	// Use same logic as worker
	subdir := ""
	if len(fileID) >= 2 {
		subdir = fileID[:2]
	}
	return filepath.Join(sw.cacheDir, subdir, fileID+".cache")
}

// HasCachedData checks if data exists in cache at offset
func (sw *ShadowWriter) HasCachedData(fileID string, offset, size int64) bool {
	cachePath := sw.GetCachePath(fileID)

	// Check if file exists
	info, err := os.Stat(cachePath)
	if err != nil {
		return false
	}

	// Check if offset+size is within file size
	return offset+size <= info.Size()
}

// ReadCachedData reads data from cache if available
func (sw *ShadowWriter) ReadCachedData(fileID string, offset, size int64) ([]byte, error) {
	cachePath := sw.GetCachePath(fileID)

	file, err := os.Open(cachePath)
	if err != nil {
		return nil, fmt.Errorf("open cache file: %w", err)
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	buf := make([]byte, size)
	n, err := file.ReadAt(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("read from cache: %w", err)
	}

	return buf[:n], nil
}

// SetWorkerCount updates the number of workers (must be called before Start)
func (sw *ShadowWriter) SetWorkerCount(count int) {
	if !sw.started.Load() && count > 0 {
		sw.workerCount = count
		sw.workers = make([]*shadowWorker, count)
	}
}

// SetQueueSize updates the queue size (must be called before Start)
func (sw *ShadowWriter) SetQueueSize(size int) {
	if !sw.started.Load() && size > 0 {
		sw.queueSize = size
		sw.queue = make(chan *WriteOperation, size)
	}
}
