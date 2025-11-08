// Package lightning implements the Lightning Stream architecture:
// Zero-copy streaming with hot/cold tiered cache for ultra-fast playback
package lightning

import (
	"context"
	"io"
	"runtime"
	"sync/atomic"
)

// Platform-specific constants
const (
	PlatformLinux  = "linux"
	PlatformDarwin = "darwin"
	PlatformBSD    = "freebsd"
)

// ZeroCopyCapability describes platform zero-copy support
type ZeroCopyCapability struct {
	Platform           string
	HasIOUring         bool // Linux 5.1+
	HasSplice          bool // Linux 2.6.17+
	HasSendfile        bool // Linux 2.2+, macOS 10.5+, FreeBSD 3.0+
	HasKqueue          bool // macOS, FreeBSD
	MaxZeroCopySize    int64
	SupportsPipeRelay  bool
	RecommendedBuffers int
}

// GetPlatformCapability returns zero-copy capabilities for current platform
func GetPlatformCapability() *ZeroCopyCapability {
	cp := &ZeroCopyCapability{
		Platform: runtime.GOOS,
	}

	switch runtime.GOOS {
	case PlatformLinux:
		cp.HasIOUring = checkIOUringSupport()
		cp.HasSplice = true // Available since kernel 2.6.17
		cp.HasSendfile = true
		cp.MaxZeroCopySize = 2 * 1024 * 1024 * 1024 // 2GB
		cp.SupportsPipeRelay = true
		cp.RecommendedBuffers = 8 // io_uring registered buffers

	case PlatformDarwin:
		cp.HasIOUring = false
		cp.HasSplice = false
		cp.HasSendfile = true // Available since 10.5
		cp.HasKqueue = true
		cp.MaxZeroCopySize = 1 * 1024 * 1024 * 1024 // 1GB
		cp.SupportsPipeRelay = false
		cp.RecommendedBuffers = 8 // Standard ring buffer

	case PlatformBSD:
		cp.HasIOUring = false
		cp.HasSplice = false
		cp.HasSendfile = true
		cp.HasKqueue = true
		cp.MaxZeroCopySize = 1 * 1024 * 1024 * 1024 // 1GB
		cp.SupportsPipeRelay = false
		cp.RecommendedBuffers = 8

	default:
		// Generic fallback
		cp.RecommendedBuffers = 4
	}

	return cp
}

// checkIOUringSupport checks if io_uring is available
func checkIOUringSupport() bool {
	// Try to probe io_uring availability
	// This would need actual io_uring probe code
	// For now, check kernel version >= 5.1
	return checkKernelVersion(5, 1)
}

// ZeroCopyReader is the platform-agnostic interface for zero-copy streaming
type ZeroCopyReader interface {
	// Read reads data with zero-copy when possible
	Read(ctx context.Context, p []byte) (n int, err error)

	// ReadAt reads from specific offset
	ReadAt(ctx context.Context, p []byte, offset int64) (n int, err error)

	// Close releases resources
	Close() error

	// Stats returns performance statistics
	Stats() *ReaderStats
}

// ReaderStats tracks zero-copy performance
type ReaderStats struct {
	BytesRead      atomic.Int64 // Total bytes read
	ZeroCopyBytes  atomic.Int64 // Bytes transferred via zero-copy
	RegularBytes   atomic.Int64 // Bytes via regular copy
	ZeroCopyOps    atomic.Int64 // Number of zero-copy operations
	RegularOps     atomic.Int64 // Number of regular operations
	FallbackReads  atomic.Int64 // Number of fallback (non-zero-copy) reads
	AverageLatency atomic.Int64 // Average latency in microseconds
}

// ZeroCopyEfficiency returns the percentage of data transferred via zero-copy
func (s *ReaderStats) ZeroCopyEfficiency() float64 {
	total := s.BytesRead.Load()
	if total == 0 {
		return 0
	}
	zeroCopy := s.ZeroCopyBytes.Load()
	return float64(zeroCopy) / float64(total) * 100
}

// PlatformReader creates the best reader for current platform
type PlatformReader struct {
	impl       ZeroCopyReader
	capability *ZeroCopyCapability
	source     io.ReadCloser
}

// NewPlatformReader creates a platform-optimized reader
func NewPlatformReader(ctx context.Context, source io.ReadCloser) (*PlatformReader, error) {
	cp := GetPlatformCapability()

	pr := &PlatformReader{
		capability: cp,
		source:     source,
	}

	var impl ZeroCopyReader
	var err error

	// Select implementation based on platform capabilities
	switch runtime.GOOS {
	case PlatformLinux:
		if cp.HasIOUring {
			// Try io_uring first (kernel 5.1+)
			impl, err = NewIOUringReader(ctx, source)
			if err == nil {
				// Successfully created io_uring reader
				pr.impl = impl
				return pr, nil
			}
			// io_uring failed, fall back to ring buffer
		}
		// Use ring buffer fallback for Linux without io_uring
		impl, err = NewRingBufferReader(ctx, source, cp.RecommendedBuffers)

	case PlatformDarwin:
		// macOS: Use ring buffer (could add kqueue in future)
		impl, err = NewRingBufferReader(ctx, source, cp.RecommendedBuffers)

	default:
		// Generic fallback for all other platforms
		impl, err = NewRingBufferReader(ctx, source, cp.RecommendedBuffers)
	}

	if err != nil {
		return nil, err
	}

	pr.impl = impl
	return pr, nil
}

// Read implements io.Reader
func (pr *PlatformReader) Read(ctx context.Context, p []byte) (int, error) {
	return pr.impl.Read(ctx, p)
}

// ReadAt implements io.ReaderAt
func (pr *PlatformReader) ReadAt(ctx context.Context, p []byte, offset int64) (int, error) {
	return pr.impl.ReadAt(ctx, p, offset)
}

// Close closes the reader
func (pr *PlatformReader) Close() error {
	return pr.impl.Close()
}

// Stats returns performance statistics
func (pr *PlatformReader) Stats() *ReaderStats {
	return pr.impl.Stats()
}

// Capability returns platform capability
func (pr *PlatformReader) Capability() *ZeroCopyCapability {
	return pr.capability
}
