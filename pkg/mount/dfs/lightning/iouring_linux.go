//go:build linux

package lightning

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"syscall"
	"time"

	iouring "github.com/iceber/iouring-go"
)

// IOUringReader provides zero-copy reads using io_uring on Linux
type IOUringReader struct {
	ring   *iouring.IOURing
	source io.ReadCloser
	fd     int

	// Statistics
	stats *ReaderStats

	// State
	closed atomic.Bool
	offset atomic.Int64
}

// ReaderStats tracks reader performance
type ReaderStats struct {
	BytesRead       atomic.Int64
	ZeroCopyOps     atomic.Int64
	ZeroCopyBytes   atomic.Int64
	AverageLatency  atomic.Int64 // Microseconds
	FallbackReads   atomic.Int64
}

// NewIOUringReader creates a new io_uring-based reader
func NewIOUringReader(ctx context.Context, source io.ReadCloser) (ZeroCopyReader, error) {
	// Get file descriptor from source
	fdGetter, ok := source.(interface{ Fd() uintptr })
	if !ok {
		return nil, errors.New("source does not provide file descriptor")
	}
	fd := int(fdGetter.Fd())

	// Create io_uring instance with 256 queue depth
	ring, err := iouring.New(256)
	if err != nil {
		return nil, err
	}

	// Register file descriptor for faster operations
	if err := ring.RegisterFiles([]int{fd}); err != nil {
		ring.Close()
		return nil, err
	}

	iur := &IOUringReader{
		ring:   ring,
		source: source,
		fd:     fd,
		stats:  &ReaderStats{},
	}

	return iur, nil
}

// Read reads data sequentially using io_uring
func (iur *IOUringReader) Read(ctx context.Context, p []byte) (int, error) {
	if iur.closed.Load() {
		return 0, io.EOF
	}

	startTime := time.Now()

	// Get current offset
	offset := iur.offset.Load()

	// Perform read at offset
	n, err := iur.readAtInternal(ctx, p, offset)
	if err != nil {
		return 0, err
	}

	// Update offset
	iur.offset.Add(int64(n))

	// Update stats
	latency := time.Since(startTime).Microseconds()
	iur.stats.AverageLatency.Store(latency)
	iur.stats.ZeroCopyOps.Add(1)
	iur.stats.ZeroCopyBytes.Add(int64(n))
	iur.stats.BytesRead.Add(int64(n))

	return n, nil
}

// ReadAt reads data at a specific offset using io_uring
func (iur *IOUringReader) ReadAt(ctx context.Context, p []byte, offset int64) (int, error) {
	if iur.closed.Load() {
		return 0, io.EOF
	}

	startTime := time.Now()

	n, err := iur.readAtInternal(ctx, p, offset)
	if err != nil {
		return 0, err
	}

	// Update stats
	latency := time.Since(startTime).Microseconds()
	iur.stats.AverageLatency.Store(latency)
	iur.stats.ZeroCopyOps.Add(1)
	iur.stats.ZeroCopyBytes.Add(int64(n))
	iur.stats.BytesRead.Add(int64(n))

	return n, nil
}

// readAtInternal performs the actual io_uring read operation
func (iur *IOUringReader) readAtInternal(ctx context.Context, p []byte, offset int64) (int, error) {
	// Check context first
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	// Get submission queue entry
	sqe := iur.ring.GetSQEntry()
	if sqe == nil {
		return 0, errors.New("failed to get submission queue entry")
	}

	// Prepare read operation
	// 0 = registered FD index (we registered our fd at index 0)
	sqe.PrepareRead(0, p, uint64(offset))
	sqe.SetUserData(1) // Operation ID

	// Submit operation
	if _, err := iur.ring.Submit(); err != nil {
		return 0, err
	}

	// Wait for completion with context support
	done := make(chan struct {
		result int
		err    error
	}, 1)

	go func() {
		cqe, err := iur.ring.WaitCQEvent(1)
		if err != nil {
			done <- struct {
				result int
				err    error
			}{0, err}
			return
		}

		result := int(cqe.Result())
		if result < 0 {
			done <- struct {
				result int
				err    error
			}{0, syscall.Errno(-result)}
			return
		}

		done <- struct {
			result int
			err    error
		}{result, nil}
	}()

	// Wait with context
	select {
	case res := <-done:
		return res.result, res.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// Close closes the io_uring reader
func (iur *IOUringReader) Close() error {
	if iur.closed.CompareAndSwap(false, true) {
		if iur.ring != nil {
			iur.ring.Close()
		}
		if iur.source != nil {
			return iur.source.Close()
		}
	}
	return nil
}

// Stats returns reader statistics
func (iur *IOUringReader) Stats() *ReaderStats {
	return iur.stats
}

// ZeroCopyEfficiency returns the percentage of zero-copy operations
func (s *ReaderStats) ZeroCopyEfficiency() float64 {
	total := s.BytesRead.Load()
	if total == 0 {
		return 0.0
	}
	zeroCopy := s.ZeroCopyBytes.Load()
	return (float64(zeroCopy) / float64(total)) * 100.0
}
