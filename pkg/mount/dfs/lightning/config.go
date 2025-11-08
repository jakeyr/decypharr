package lightning

import (
	"fmt"
	"time"
)

// Config configures the Lightning streaming system
type Config struct {
	// Hot Path Configuration
	EnableZeroCopy  bool  `json:"enable_zero_copy"`  // Enable platform-specific zero-copy
	RingBufferSize  int   `json:"ring_buffer_size"`  // Number of buffers (default: 8)
	RingBufferChunk int64 `json:"ring_buffer_chunk"` // Size per buffer (default: 512KB)

	// Connection Pool Configuration
	PoolSize          int           `json:"pool_size"`          // Number of HTTP/2 connections (default: 8)
	ConnectionTimeout time.Duration `json:"connection_timeout"` // Connection timeout
	RequestTimeout    time.Duration `json:"request_timeout"`    // Request timeout
	EnableHTTP2       bool          `json:"enable_http2"`       // Enable HTTP/2
	EnableKeepalive   bool          `json:"enable_keepalive"`   // Enable TCP keepalive

	// Readahead Configuration
	InitialReadahead int64 `json:"initial_readahead"` // Starting window (default: 256KB)
	MaxReadahead     int64 `json:"max_readahead"`     // Max window (default: 4MB)
	MinReadahead     int64 `json:"min_readahead"`     // Min window (default: 128KB)
	EnableReadahead  bool  `json:"enable_readahead"`  // Enable adaptive readahead

	// Memory Configuration
	HotBufferMem   int64 `json:"hot_buffer_mem"`   // Hot buffer memory (default: 64MB)
	MemCacheSize   int64 `json:"mem_cache_size"`   // Cold cache memory (default: 256MB)
	ReadaheadMem   int64 `json:"readahead_mem"`    // Readahead memory (default: 128MB)
	TotalMemBudget int64 `json:"total_mem_budget"` // Total memory budget (default: 448MB)

	// Cold Path Configuration
	ChunkSize       int64  `json:"chunk_size"`        // Chunk size (default: 4MB)
	DiskCacheDir    string `json:"disk_cache_dir"`    // Disk cache directory
	DiskCacheSize   int64  `json:"disk_cache_size"`   // Max disk cache size (0 = unlimited)
	EnableDiskCache bool   `json:"enable_disk_cache"` // Enable disk caching

	// Router Configuration
	SequentialThreshold int64   `json:"sequential_threshold"` // Sequential gap tolerance (default: 128KB)
	HotPathMinSize      int64   `json:"hot_path_min_size"`    // Min size for hot path (default: 4MB)
	ConfidenceThreshold float64 `json:"confidence_threshold"` // Pattern confidence (default: 0.85)

	// Shadow Writer Configuration
	ShadowWorkers     int  `json:"shadow_workers"`      // Number of shadow writer workers (default: 4)
	ShadowQueueSize   int  `json:"shadow_queue_size"`   // Shadow write queue size (default: 256)
	EnableShadowCache bool `json:"enable_shadow_cache"` // Enable shadow caching

	// Performance Tuning
	MaxConcurrentReads int  `json:"max_concurrent_reads"` // Max concurrent read operations
	TCPNoDelay         bool `json:"tcp_no_delay"`         // Disable Nagle's algorithm
	TCPBufferSize      int  `json:"tcp_buffer_size"`      // TCP buffer size (SO_RCVBUF)
}

// DefaultConfig returns the default Lightning configuration
func DefaultConfig() *Config {
	return &Config{
		// Hot Path
		EnableZeroCopy:  true,
		RingBufferSize:  8,
		RingBufferChunk: 512 * 1024, // 512KB

		// Connection Pool
		PoolSize:          8,
		ConnectionTimeout: 30 * time.Second,
		RequestTimeout:    60 * time.Second,
		EnableHTTP2:       true,
		EnableKeepalive:   true,

		// Readahead
		InitialReadahead: 256 * 1024,      // 256KB
		MaxReadahead:     4 * 1024 * 1024, // 4MB
		MinReadahead:     128 * 1024,      // 128KB
		EnableReadahead:  true,

		// Memory
		HotBufferMem:   64 * 1024 * 1024,  // 64MB
		MemCacheSize:   256 * 1024 * 1024, // 256MB
		ReadaheadMem:   128 * 1024 * 1024, // 128MB
		TotalMemBudget: 448 * 1024 * 1024, // 448MB

		// Cold Path
		ChunkSize:       4 * 1024 * 1024, // 4MB
		DiskCacheDir:    "/tmp/lightning-cache",
		DiskCacheSize:   5 * 1024 * 1024 * 1024, // 5GB
		EnableDiskCache: true,

		// Router
		SequentialThreshold: 128 * 1024,      // 128KB
		HotPathMinSize:      4 * 1024 * 1024, // 4MB
		ConfidenceThreshold: 0.85,

		// Shadow Writer
		ShadowWorkers:     4,
		ShadowQueueSize:   256,
		EnableShadowCache: true,

		// Performance
		MaxConcurrentReads: 32,
		TCPNoDelay:         true,
		TCPBufferSize:      4 * 1024 * 1024, // 4MB
	}
}

// MinimalConfig returns a minimal configuration for low-resource systems
func MinimalConfig() *Config {
	return &Config{
		// Hot Path
		EnableZeroCopy:  true,
		RingBufferSize:  4,
		RingBufferChunk: 256 * 1024, // 256KB

		// Connection Pool
		PoolSize:          4,
		ConnectionTimeout: 30 * time.Second,
		RequestTimeout:    60 * time.Second,
		EnableHTTP2:       true,
		EnableKeepalive:   true,

		// Readahead
		InitialReadahead: 128 * 1024,      // 128KB
		MaxReadahead:     1 * 1024 * 1024, // 1MB
		MinReadahead:     64 * 1024,       // 64KB
		EnableReadahead:  true,

		// Memory
		HotBufferMem:   16 * 1024 * 1024,  // 16MB
		MemCacheSize:   64 * 1024 * 1024,  // 64MB
		ReadaheadMem:   32 * 1024 * 1024,  // 32MB
		TotalMemBudget: 112 * 1024 * 1024, // 112MB

		// Cold Path
		ChunkSize:       2 * 1024 * 1024, // 2MB
		DiskCacheDir:    "/tmp/lightning-cache",
		DiskCacheSize:   1 * 1024 * 1024 * 1024, // 1GB
		EnableDiskCache: true,

		// Router
		SequentialThreshold: 64 * 1024,       // 64KB
		HotPathMinSize:      2 * 1024 * 1024, // 2MB
		ConfidenceThreshold: 0.80,

		// Shadow Writer
		ShadowWorkers:     2,
		ShadowQueueSize:   128,
		EnableShadowCache: true,

		// Performance
		MaxConcurrentReads: 16,
		TCPNoDelay:         true,
		TCPBufferSize:      2 * 1024 * 1024, // 2MB
	}
}

// HighPerformanceConfig returns a configuration optimized for high performance
func HighPerformanceConfig() *Config {
	return &Config{
		// Hot Path
		EnableZeroCopy:  true,
		RingBufferSize:  16,
		RingBufferChunk: 1024 * 1024, // 1MB

		// Connection Pool
		PoolSize:          16,
		ConnectionTimeout: 30 * time.Second,
		RequestTimeout:    120 * time.Second,
		EnableHTTP2:       true,
		EnableKeepalive:   true,

		// Readahead
		InitialReadahead: 512 * 1024,      // 512KB
		MaxReadahead:     8 * 1024 * 1024, // 8MB
		MinReadahead:     256 * 1024,      // 256KB
		EnableReadahead:  true,

		// Memory
		HotBufferMem:   128 * 1024 * 1024, // 128MB
		MemCacheSize:   512 * 1024 * 1024, // 512MB
		ReadaheadMem:   256 * 1024 * 1024, // 256MB
		TotalMemBudget: 896 * 1024 * 1024, // 896MB

		// Cold Path
		ChunkSize:       8 * 1024 * 1024, // 8MB
		DiskCacheDir:    "/tmp/lightning-cache",
		DiskCacheSize:   20 * 1024 * 1024 * 1024, // 20GB
		EnableDiskCache: true,

		// Router
		SequentialThreshold: 256 * 1024,      // 256KB
		HotPathMinSize:      2 * 1024 * 1024, // 2MB
		ConfidenceThreshold: 0.90,

		// Shadow Writer
		ShadowWorkers:     8,
		ShadowQueueSize:   512,
		EnableShadowCache: true,

		// Performance
		MaxConcurrentReads: 64,
		TCPNoDelay:         true,
		TCPBufferSize:      8 * 1024 * 1024, // 8MB
	}
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.PoolSize <= 0 {
		c.PoolSize = 8
	}
	if c.RingBufferSize <= 0 {
		c.RingBufferSize = 8
	}
	if c.RingBufferChunk <= 0 {
		c.RingBufferChunk = 512 * 1024
	}
	if c.ChunkSize <= 0 {
		c.ChunkSize = 4 * 1024 * 1024
	}
	if c.MaxReadahead < c.MinReadahead {
		c.MaxReadahead = c.MinReadahead * 4
	}
	if c.InitialReadahead < c.MinReadahead {
		c.InitialReadahead = c.MinReadahead
	}
	if c.InitialReadahead > c.MaxReadahead {
		c.InitialReadahead = c.MaxReadahead
	}
	if c.ConfidenceThreshold <= 0 || c.ConfidenceThreshold > 1 {
		c.ConfidenceThreshold = 0.85
	}
	if c.TotalMemBudget <= 0 {
		c.TotalMemBudget = c.HotBufferMem + c.MemCacheSize + c.ReadaheadMem
	}
	if c.ShadowWorkers <= 0 {
		c.ShadowWorkers = 4
	}
	if c.ShadowQueueSize <= 0 {
		c.ShadowQueueSize = 256
	}
	if c.MaxConcurrentReads <= 0 {
		c.MaxConcurrentReads = 32
	}

	return nil
}

// Clone creates a copy of the configuration
func (c *Config) Clone() *Config {
	_copy := *c
	return &_copy
}

// String returns a human-readable string representation
func (c *Config) String() string {
	return fmt.Sprintf(`Lightning Configuration:
  Hot Path:
    Zero-Copy: %v
    Ring Buffers: %d × %s

  Connection Pool:
    Size: %d connections
    HTTP/2: %v
    Timeout: %v

  Memory Budget:
    Total: %s
    Hot Buffers: %s
    Memory Cache: %s
    Readahead: %s

  Readahead:
    Enabled: %v
    Range: %s - %s

  Cold Path:
    Chunk Size: %s
    Disk Cache: %v (%s)

  Performance:
    Max Concurrent: %d
    TCP NoDelay: %v`,
		c.EnableZeroCopy,
		c.RingBufferSize, formatBytes(c.RingBufferChunk),
		c.PoolSize,
		c.EnableHTTP2,
		c.ConnectionTimeout,
		formatBytes(c.TotalMemBudget),
		formatBytes(c.HotBufferMem),
		formatBytes(c.MemCacheSize),
		formatBytes(c.ReadaheadMem),
		c.EnableReadahead,
		formatBytes(c.MinReadahead), formatBytes(c.MaxReadahead),
		formatBytes(c.ChunkSize),
		c.EnableDiskCache, formatBytes(c.DiskCacheSize),
		c.MaxConcurrentReads,
		c.TCPNoDelay,
	)
}

// formatBytes formats byte count in human-readable form
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
