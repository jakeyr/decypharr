//go:build linux

package lightning

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// SpliceReader implements zero-copy streaming using Linux splice
type SpliceReader struct {
	source io.ReadCloser
	ctx    context.Context
	cancel context.CancelFunc

	// Splice pipe
	pipeR int // Pipe read end
	pipeW int // Pipe write end

	// Source file descriptor (if available)
	sourceFD int
	hasFD    bool

	// State
	stats     *ReaderStats
	closed    atomic.Bool
	readDone  atomic.Bool
	err       atomic.Value
	bytesRead atomic.Int64

	// Buffer for non-FD sources
	buffer     []byte
	bufferSize int

	// Synchronization
	mu sync.Mutex
	wg sync.WaitGroup
}

const (
	defaultPipeSize = 1 * 1024 * 1024 // 1MB pipe buffer
	spliceChunkSize = 512 * 1024      // 512KB per splice
)

// NewSpliceReader creates a Linux splice-based reader
func NewSpliceReader(ctx context.Context, source io.ReadCloser) (ZeroCopyReader, error) {
	if !hasSpliceSupport() {
		return nil, fmt.Errorf("splice not supported on this system")
	}

	ctx, cancel := context.WithCancel(ctx)

	sr := &SpliceReader{
		source:     source,
		ctx:        ctx,
		cancel:     cancel,
		stats:      &ReaderStats{},
		bufferSize: 64 * 1024, // 64KB for non-FD fallback
	}

	// Create pipe for splice
	var err error
	sr.pipeR, sr.pipeW, err = CreatePipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create pipe: %w", err)
	}

	// Try to increase pipe size
	if err := SetPipeSize(sr.pipeW, defaultPipeSize); err != nil {
		// Non-fatal, use default size
	}

	// Check if source has file descriptor
	if f, ok := source.(*os.File); ok {
		sr.sourceFD = GetFD(f)
		sr.hasFD = true

		// Set non-blocking mode for better performance
		if err := SetNonBlocking(sr.sourceFD); err == nil {
			// Successfully set non-blocking
		}
	} else {
		// No FD, will need buffer fallback
		sr.buffer = make([]byte, sr.bufferSize)
	}

	return sr, nil
}

// Read implements ZeroCopyReader
func (sr *SpliceReader) Read(ctx context.Context, p []byte) (int, error) {
	if sr.closed.Load() {
		return 0, io.EOF
	}

	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.hasFD {
		// Use splice for zero-copy
		return sr.spliceRead(ctx, p)
	}

	// Fallback to regular read
	return sr.regularRead(ctx, p)
}

// spliceRead performs zero-copy read using splice
func (sr *SpliceReader) spliceRead(ctx context.Context, p []byte) (int, error) {
	startTime := time.Now()

	// First, splice from source FD to pipe
	toRead := len(p)
	if toRead > spliceChunkSize {
		toRead = spliceChunkSize
	}

	// Splice: source FD → pipe write end
	n, err := Splice(
		sr.sourceFD, nil,
		sr.pipeW, nil,
		toRead,
		SPLICE_F_MOVE|SPLICE_F_NONBLOCK,
	)

	if err != nil {
		if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
			// Would block, return what we have
			return 0, nil
		}
		if errors.Is(err, io.EOF) {
			sr.readDone.Store(true)
			return 0, io.EOF
		}
		return 0, fmt.Errorf("splice to pipe: %w", err)
	}

	if n == 0 {
		sr.readDone.Store(true)
		return 0, io.EOF
	}

	// Now read from pipe to user buffer
	// Note: This is one copy, but we avoided source→kernel→user copy
	bytesRead, err := syscall.Read(sr.pipeR, p[:n])
	if err != nil {
		return bytesRead, fmt.Errorf("read from pipe: %w", err)
	}

	// Update stats
	latency := time.Since(startTime).Microseconds()
	sr.stats.AverageLatency.Store(latency)
	sr.stats.ZeroCopyOps.Add(1)
	sr.stats.ZeroCopyBytes.Add(int64(bytesRead))
	sr.stats.BytesRead.Add(int64(bytesRead))
	sr.bytesRead.Add(int64(bytesRead))

	return bytesRead, nil
}

// regularRead fallback for non-FD sources
func (sr *SpliceReader) regularRead(ctx context.Context, p []byte) (int, error) {
	startTime := time.Now()

	n, err := sr.source.Read(p)

	// Update stats
	latency := time.Since(startTime).Microseconds()
	sr.stats.AverageLatency.Store(latency)
	sr.stats.RegularOps.Add(1)
	sr.stats.RegularBytes.Add(int64(n))
	sr.stats.BytesRead.Add(int64(n))
	sr.bytesRead.Add(int64(n))

	if err != nil {
		if errors.Is(err, io.EOF) {
			sr.readDone.Store(true)
		}
		return n, err
	}

	return n, nil
}

// ReadAt implements ZeroCopyReader
func (sr *SpliceReader) ReadAt(ctx context.Context, p []byte, offset int64) (int, error) {
	// splice doesn't naturally support random access on sockets
	// For file-backed sources, we could use pread + splice
	// For now, return not supported
	return 0, fmt.Errorf("splice reader does not support random access")
}

// Close implements ZeroCopyReader
func (sr *SpliceReader) Close() error {
	if sr.closed.CompareAndSwap(false, true) {
		sr.cancel()

		// Wait for any ongoing operations
		sr.wg.Wait()

		// Close pipe
		if sr.pipeR > 0 {
			syscall.Close(sr.pipeR)
		}
		if sr.pipeW > 0 {
			syscall.Close(sr.pipeW)
		}

		// Close source
		if sr.source != nil {
			return sr.source.Close()
		}
	}
	return nil
}

// Stats implements ZeroCopyReader
func (sr *SpliceReader) Stats() *ReaderStats {
	return sr.stats
}
