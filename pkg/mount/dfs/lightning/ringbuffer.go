package lightning

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// RingBufferReader implements zero-copy-style streaming using ring buffer
// This is the cross-platform fallback that works on all operating systems
type RingBufferReader struct {
	source io.ReadCloser
	ctx    context.Context
	cancel context.CancelFunc

	// Ring buffer configuration
	bufferSize  int       // Size of each buffer (512KB default)
	bufferCount int       // Number of buffers in ring (8 default)
	buffers     [][]byte  // The actual buffers
	bufferCh    chan int  // Channel of available buffer indices
	dataCh      chan *dataChunk // Channel of filled buffers

	// Buffer pool for recycling
	pool *sync.Pool

	// State
	stats     *ReaderStats
	closed    atomic.Bool
	readDone  atomic.Bool
	err       atomic.Value // stores error
	bytesRead atomic.Int64

	// Background reader goroutine
	readerWg sync.WaitGroup
}

// dataChunk represents a filled buffer
type dataChunk struct {
	buf    []byte // Buffer data
	n      int    // Bytes in buffer
	offset int64  // Offset of this chunk in stream
}

const (
	defaultBufferSize  = 512 * 1024 // 512KB per buffer
	defaultBufferCount = 8          // 8 buffers = 4MB total
)

// NewRingBufferReader creates a ring buffer reader
func NewRingBufferReader(ctx context.Context, source io.ReadCloser, bufferCount int) (ZeroCopyReader, error) {
	if bufferCount <= 0 {
		bufferCount = defaultBufferCount
	}

	ctx, cancel := context.WithCancel(ctx)

	rbr := &RingBufferReader{
		source:      source,
		ctx:         ctx,
		cancel:      cancel,
		bufferSize:  defaultBufferSize,
		bufferCount: bufferCount,
		buffers:     make([][]byte, bufferCount),
		bufferCh:    make(chan int, bufferCount),
		dataCh:      make(chan *dataChunk, bufferCount),
		stats:       &ReaderStats{},
		pool: &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, defaultBufferSize)
				return &buf
			},
		},
	}

	// Allocate buffers
	for i := 0; i < bufferCount; i++ {
		rbr.buffers[i] = make([]byte, defaultBufferSize)
		rbr.bufferCh <- i // All buffers start available
	}

	// Start background reader
	rbr.readerWg.Add(1)
	go rbr.readLoop()

	return rbr, nil
}

// readLoop continuously reads from source and fills buffers
func (rbr *RingBufferReader) readLoop() {
	defer rbr.readerWg.Done()
	defer close(rbr.dataCh)

	offset := int64(0)
	var readErr error

	for {
		// Check if closed
		if rbr.closed.Load() {
			break
		}

		// Get an available buffer
		var bufIdx int
		select {
		case bufIdx = <-rbr.bufferCh:
			// Got a buffer
		case <-rbr.ctx.Done():
			readErr = rbr.ctx.Err()
			break
		}

		buf := rbr.buffers[bufIdx]

		// Read from source
		startTime := time.Now()
		n, err := rbr.source.Read(buf)
		latency := time.Since(startTime).Microseconds()

		// Update stats
		rbr.stats.AverageLatency.Store(latency)
		rbr.stats.RegularOps.Add(1)
		rbr.stats.RegularBytes.Add(int64(n))
		rbr.stats.BytesRead.Add(int64(n))

		if n > 0 {
			// Send filled buffer to data channel
			chunk := &dataChunk{
				buf:    buf[:n],
				n:      n,
				offset: offset,
			}

			select {
			case rbr.dataCh <- chunk:
				offset += int64(n)
			case <-rbr.ctx.Done():
				readErr = rbr.ctx.Err()
				rbr.bufferCh <- bufIdx // Return buffer
				break
			}
		} else {
			// No data read, return buffer
			rbr.bufferCh <- bufIdx
		}

		// Check for errors
		if err != nil {
			if errors.Is(err, io.EOF) {
				readErr = io.EOF
			} else {
				readErr = fmt.Errorf("read from source: %w", err)
			}
			break
		}
	}

	// Mark read as done and store error
	rbr.readDone.Store(true)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		rbr.err.Store(readErr)
	}
}

// Read implements ZeroCopyReader
func (rbr *RingBufferReader) Read(ctx context.Context, p []byte) (int, error) {
	if rbr.closed.Load() {
		return 0, io.EOF
	}

	// Wait for data from channel
	select {
	case chunk, ok := <-rbr.dataCh:
		if !ok {
			// Channel closed - check if there was an error
			if err := rbr.getError(); err != nil {
				return 0, err
			}
			return 0, io.EOF
		}

		// Copy data to output buffer
		n := copy(p, chunk.buf)

		// If we didn't consume all data, we need to handle remainder
		if n < chunk.n {
			// Put remainder back (this is a limitation of the ring buffer approach)
			// In practice, this shouldn't happen often as readers typically use
			// buffer sizes >= ring buffer size
			remainder := &dataChunk{
				buf:    chunk.buf[n:chunk.n],
				n:      chunk.n - n,
				offset: chunk.offset + int64(n),
			}

			// Try to put it back
			select {
			case rbr.dataCh <- remainder:
				// Successfully queued remainder
			case <-ctx.Done():
				return n, ctx.Err()
			default:
				// Channel full, we'll lose this data (should not happen)
				// Return buffer to pool
				rbr.returnBuffer(chunk.buf)
			}
		} else {
			// All data consumed, return buffer
			rbr.returnBuffer(chunk.buf)
		}

		return n, nil

	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// ReadAt implements ZeroCopyReader
// Note: Ring buffer doesn't naturally support random access
// This is a sequential implementation
func (rbr *RingBufferReader) ReadAt(ctx context.Context, p []byte, offset int64) (int, error) {
	// Ring buffer is designed for sequential reading
	// For random access, use the chunk cache path instead
	return 0, fmt.Errorf("ring buffer reader does not support random access")
}

// returnBuffer returns a buffer to the available pool
func (rbr *RingBufferReader) returnBuffer(buf []byte) {
	// Find which buffer index this is
	for i, b := range rbr.buffers {
		if len(b) > 0 && &b[0] == &buf[0] {
			// Return to channel
			select {
			case rbr.bufferCh <- i:
				return
			default:
				// Channel full (shouldn't happen)
				return
			}
		}
	}
}

// Close implements ZeroCopyReader
func (rbr *RingBufferReader) Close() error {
	if rbr.closed.CompareAndSwap(false, true) {
		rbr.cancel()

		// Drain data channel to unblock reader
		go func() {
			for range rbr.dataCh {
				// Drain
			}
		}()

		// Wait for reader to finish
		rbr.readerWg.Wait()

		// Close source
		if rbr.source != nil {
			return rbr.source.Close()
		}
	}
	return nil
}

// Stats implements ZeroCopyReader
func (rbr *RingBufferReader) Stats() *ReaderStats {
	return rbr.stats
}

// getError returns any stored error
func (rbr *RingBufferReader) getError() error {
	if err := rbr.err.Load(); err != nil {
		return err.(error)
	}
	return nil
}

// BytesBuffered returns approximate bytes buffered in ring
func (rbr *RingBufferReader) BytesBuffered() int64 {
	return int64(len(rbr.dataCh)) * int64(rbr.bufferSize)
}

// IsComplete returns true if all data has been read from source
func (rbr *RingBufferReader) IsComplete() bool {
	return rbr.readDone.Load()
}
