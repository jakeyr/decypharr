package lightning

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
)

// Streamer is the main integration point for DFS
// It combines all Lightning components into a unified streaming interface
type Streamer struct {
	// Core components
	config       *Config
	connPool     *ConnectionPool
	router       *ReadRouter
	readahead    *AdaptiveReadahead
	shadowWriter *ShadowWriter
	chunkCache   *ChunkCache
	memBudget    *MemoryBudget

	// Per-file state (lock-free concurrent map)
	files *xsync.Map[string, *FileStream]

	// Platform reader factory
	platformReader *PlatformReader

	// State
	started atomic.Bool
	closed  atomic.Bool
}

// FileStream represents a streaming session for a single file
type FileStream struct {
	fileID   string
	fileSize int64
	url      string

	// Hot path streaming
	hotReader ZeroCopyReader

	// State
	lastOffset atomic.Int64
	mu         sync.RWMutex
}

// NewLightningStreamer creates a new Lightning streaming system
func NewLightningStreamer(ctx context.Context, config *Config, serverURL string) (*Streamer, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	ls := &Streamer{
		config: config,
		files:  xsync.NewMap[string, *FileStream](),
	}

	// Create memory budget manager
	ls.memBudget = NewMemoryBudget(config.TotalMemBudget)

	// Create connection pool
	connPool, err := NewConnectionPool(ctx, serverURL, config.PoolSize)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}
	ls.connPool = connPool

	// Create read router
	ls.router = NewReadRouter()
	ls.router.SetConfiguration(
		config.SequentialThreshold,
		config.HotPathMinSize,
		config.ConfidenceThreshold,
	)

	// Create adaptive readahead
	if config.EnableReadahead {
		ls.readahead = NewAdaptiveReadahead(config.ReadaheadMem)
		ls.readahead.SetWindowLimits(config.MinReadahead, config.MaxReadahead)
	}

	// Create shadow writer
	if config.EnableShadowCache && config.DiskCacheDir != "" {
		shadowWriter, err := NewShadowWriter(ctx, config.DiskCacheDir)
		if err != nil {
			return nil, fmt.Errorf("create shadow writer: %w", err)
		}
		shadowWriter.SetWorkerCount(config.ShadowWorkers)
		shadowWriter.SetQueueSize(config.ShadowQueueSize)
		if err := shadowWriter.Start(); err != nil {
			return nil, fmt.Errorf("start shadow writer: %w", err)
		}
		ls.shadowWriter = shadowWriter
	}

	// Create chunk cache
	ls.chunkCache = NewChunkCache(ls.connPool, ls.shadowWriter, config.MemCacheSize)

	ls.started.Store(true)

	return ls, nil
}

// OpenFile opens a file for streaming
func (ls *Streamer) OpenFile(fileID string, fileSize int64, url string) (*FileStream, error) {
	if ls.closed.Load() {
		return nil, fmt.Errorf("streamer is closed")
	}

	// Create file stream
	fs := &FileStream{
		fileID:   fileID,
		fileSize: fileSize,
		url:      url,
	}

	// Try to load or store (lock-free, atomic operation)
	actual, loaded := ls.files.LoadOrStore(fileID, fs)
	if loaded {
		// File was already opened by another goroutine
		return actual, nil
	}

	// We created it successfully
	return fs, nil
}

// ReadAt reads from a file using hot or cold path based on pattern
func (ls *Streamer) ReadAt(ctx context.Context, fileID string, p []byte, offset int64) (int, error) {
	if ls.closed.Load() {
		return 0, fmt.Errorf("streamer is closed")
	}

	size := int64(len(p))

	// Determine hot path or cold path
	useHotPath := ls.router.ShouldUseHotPath(fileID, offset, size)

	if useHotPath {
		// Hot path: Zero-copy streaming
		return ls.hotPathRead(ctx, fileID, p, offset)
	} else {
		// Cold path: Chunk cache
		return ls.coldPathRead(ctx, fileID, p, offset)
	}
}

// hotPathRead performs streaming read with zero-copy
func (ls *Streamer) hotPathRead(ctx context.Context, fileID string, p []byte, offset int64) (int, error) {
	// Get file stream
	fs, err := ls.getFileStream(fileID)
	if err != nil {
		return 0, err
	}

	// Download data via connection pool
	size := int64(len(p))
	reader, err := ls.connPool.Download(ctx, offset, size)
	if err != nil {
		return 0, fmt.Errorf("download: %w", err)
	}
	defer func(reader io.ReadCloser) {
		_ = reader.Close()
	}(reader)

	// Create platform reader for zero-copy if not exists
	if ls.config.EnableZeroCopy && fs.hotReader == nil {
		platformReader, err := NewPlatformReader(ctx, reader)
		if err == nil {
			fs.mu.Lock()
			fs.hotReader = platformReader
			fs.mu.Unlock()
		}
	}

	// Read data
	var n int
	if fs.hotReader != nil {
		n, err = fs.hotReader.Read(ctx, p)
	} else {
		n, err = io.ReadFull(reader, p)
		if errors.Is(err, io.ErrUnexpectedEOF) || err == io.EOF {
			err = nil
		}
	}

	if n > 0 && ls.shadowWriter != nil {
		// Queue shadow write (non-blocking)
		_ = ls.shadowWriter.QueueWrite(fileID, offset, p[:n])
	}

	// Update last offset
	fs.lastOffset.Store(offset + int64(n))

	// Trigger readahead if enabled
	if ls.readahead != nil && n > 0 {
		go ls.triggerReadahead(ctx, fileID, offset+int64(n))
	}

	return n, err
}

// coldPathRead performs chunk-based cached read
func (ls *Streamer) coldPathRead(ctx context.Context, fileID string, p []byte, offset int64) (int, error) {
	// Use chunk cache
	return ls.chunkCache.ReadAt(ctx, fileID, p, offset)
}

// triggerReadahead triggers readahead prefetch
func (ls *Streamer) triggerReadahead(ctx context.Context, fileID string, nextOffset int64) {
	if !ls.readahead.ShouldPrefetch() {
		return
	}

	// Start prefetch operation
	downloader := func(ctx context.Context, offset, size int64) ([]byte, error) {
		reader, err := ls.connPool.Download(ctx, offset, size)
		if err != nil {
			return nil, err
		}
		defer func(reader io.ReadCloser) {
			_ = reader.Close()
		}(reader)

		buf := make([]byte, size)
		n, err := io.ReadFull(reader, buf)
		if err != nil && err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, err
		}
		return buf[:n], nil
	}

	ls.readahead.StartPrefetch(ctx, fileID, nextOffset, downloader)
}

// getFileStream retrieves a file stream
func (ls *Streamer) getFileStream(fileID string) (*FileStream, error) {
	fs, exists := ls.files.Load(fileID)
	if !exists {
		return nil, fmt.Errorf("file not opened: %s", fileID)
	}

	return fs, nil
}

// CloseFile closes a file stream
func (ls *Streamer) CloseFile(fileID string) error {
	fs, exists := ls.files.LoadAndDelete(fileID)
	if !exists {
		return nil
	}

	// Close hot reader if exists
	if fs.hotReader != nil {
		_ = fs.hotReader.Close()
	}

	// Clear router history
	ls.router.ClearHistory(fileID)

	return nil
}

// Close closes the Lightning streamer
func (ls *Streamer) Close() error {
	if ls.closed.CompareAndSwap(false, true) {
		// Close all file streams
		ls.files.Range(func(fileID string, fs *FileStream) bool {
			if fs.hotReader != nil {
				_ = fs.hotReader.Close()
			}
			ls.router.ClearHistory(fileID)
			return true // Continue iteration
		})
		ls.files.Clear()

		// Close components
		if ls.shadowWriter != nil {
			_ = ls.shadowWriter.Close()
		}

		if ls.connPool != nil {
			_ = ls.connPool.Close()
		}

		if ls.memBudget != nil {
			ls.memBudget.Close()
		}
	}

	return nil
}

// GetStats returns comprehensive statistics
func (ls *Streamer) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})

	// Router stats
	stats["router"] = ls.router.GetStats()

	// Connection pool stats
	if ls.connPool != nil {
		poolStats := ls.connPool.Stats()
		stats["connection_pool"] = map[string]interface{}{
			"total_requests":      poolStats.TotalRequests.Load(),
			"failed_requests":     poolStats.FailedRequests.Load(),
			"healthy_connections": poolStats.HealthyConnections.Load(),
		}
	}

	// Readahead stats
	if ls.readahead != nil {
		stats["readahead"] = ls.readahead.GetStats()
	}

	// Chunk cache stats
	if ls.chunkCache != nil {
		cacheStats := ls.chunkCache.Stats()
		stats["chunk_cache"] = map[string]interface{}{
			"mem_hits":        cacheStats.MemHits.Load(),
			"disk_hits":       cacheStats.DiskHits.Load(),
			"misses":          cacheStats.Misses.Load(),
			"bytes_served_mb": cacheStats.BytesServed.Load() / (1024 * 1024),
			"chunks_loaded":   cacheStats.ChunksLoaded.Load(),
			"chunks_evicted":  cacheStats.ChunksEvicted.Load(),
		}

		// Calculate hit rate
		totalRequests := cacheStats.MemHits.Load() + cacheStats.DiskHits.Load() + cacheStats.Misses.Load()
		if totalRequests > 0 {
			hitRate := float64(cacheStats.MemHits.Load()+cacheStats.DiskHits.Load()) / float64(totalRequests) * 100
			stats["chunk_cache"].(map[string]interface{})["hit_rate_pct"] = hitRate
		}
	}

	// Shadow writer stats
	if ls.shadowWriter != nil {
		shadowStats := ls.shadowWriter.Stats()
		stats["shadow_writer"] = map[string]interface{}{
			"bytes_written_mb": shadowStats.BytesWritten.Load() / (1024 * 1024),
			"writes_completed": shadowStats.WritesCompleted.Load(),
			"writes_failed":    shadowStats.WritesFailed.Load(),
			"writes_queued":    shadowStats.WritesQueued.Load(),
			"writes_dropped":   shadowStats.WritesDropped.Load(),
			"queue_depth":      shadowStats.QueueDepth.Load(),
			"avg_latency_ms":   shadowStats.AverageLatency.Load() / 1000,
		}
	}

	// Memory budget stats
	if ls.memBudget != nil {
		stats["memory"] = ls.memBudget.GetStats()
	}

	// File count
	fileCount := 0
	ls.files.Range(func(_ string, _ *FileStream) bool {
		fileCount++
		return true
	})
	stats["open_files"] = fileCount

	return stats
}

// Flush flushes pending shadow writes
func (ls *Streamer) Flush(ctx context.Context) error {
	if ls.shadowWriter != nil {
		return ls.shadowWriter.Flush(5 * time.Second)
	}
	return nil
}

// GetConfig returns the current configuration
func (ls *Streamer) GetConfig() *Config {
	return ls.config
}

// GetPattern returns the current access pattern for a file
func (ls *Streamer) GetPattern(fileID string) (string, float64) {
	pattern, confidence := ls.router.GetPattern(fileID)
	return pattern.String(), confidence
}
