package vfs

import (
	"sync"
)

// RingBuffer provides a fast in-memory buffer for sequential reads
// Data is stored in a circular buffer with file offset tracking
type RingBuffer struct {
	mu       sync.RWMutex
	data     []byte
	size     int64
	head     int64 // Write position in buffer
	startOff int64 // File offset of first byte in buffer
	filled   int64 // Bytes currently in buffer
}

// NewRingBuffer creates a new ring buffer with the specified size
func NewRingBuffer(size int64) *RingBuffer {
	return &RingBuffer{
		data:     make([]byte, size),
		size:     size,
		startOff: -1, // No data yet
	}
}

// ReadAt tries to read from the ring buffer
// Returns (bytes read, true) if data was in buffer, (0, false) otherwise
func (rb *RingBuffer) ReadAt(p []byte, offset int64) (int, bool) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.filled == 0 || rb.startOff < 0 {
		return 0, false
	}

	// Check if requested range is in buffer
	bufferEnd := rb.startOff + rb.filled
	requestEnd := offset + int64(len(p))

	if offset < rb.startOff || requestEnd > bufferEnd {
		return 0, false
	}

	// Calculate position in ring buffer
	bufOffset := offset - rb.startOff
	readPos := (rb.head - rb.filled + bufOffset) % rb.size
	if readPos < 0 {
		readPos += rb.size
	}

	// Copy data from ring buffer
	n := 0
	for n < len(p) {
		chunk := int(rb.size - readPos)
		if chunk > len(p)-n {
			chunk = len(p) - n
		}
		copy(p[n:n+chunk], rb.data[readPos:readPos+int64(chunk)])
		n += chunk
		readPos = 0 // Wrap around
	}

	return n, true
}

// Write appends data to the ring buffer
// Updates startOff to maintain continuity for sequential reads
func (rb *RingBuffer) Write(data []byte, offset int64) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	dataLen := int64(len(data))
	if dataLen == 0 {
		return
	}

	// If this is non-sequential write, reset buffer
	if rb.startOff >= 0 && offset != rb.startOff+rb.filled {
		// Non-sequential - reset and start fresh
		rb.startOff = offset
		rb.head = 0
		rb.filled = 0
	}

	// Initialize startOff on first write
	if rb.startOff < 0 {
		rb.startOff = offset
	}

	// Write data to ring buffer
	written := int64(0)
	for written < dataLen {
		chunk := rb.size - rb.head
		if chunk > dataLen-written {
			chunk = dataLen - written
		}
		copy(rb.data[rb.head:rb.head+chunk], data[written:written+chunk])
		rb.head = (rb.head + chunk) % rb.size
		written += chunk
	}

	// Update filled amount (capped at buffer size)
	rb.filled += dataLen
	if rb.filled > rb.size {
		// Buffer wrapped - update startOff
		overflow := rb.filled - rb.size
		rb.startOff += overflow
		rb.filled = rb.size
	}
}

// Clear resets the ring buffer
func (rb *RingBuffer) Clear() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.head = 0
	rb.startOff = -1
	rb.filled = 0
}

// Contains checks if the buffer contains data for the given range
func (rb *RingBuffer) Contains(offset, size int64) bool {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.filled == 0 || rb.startOff < 0 {
		return false
	}

	bufferEnd := rb.startOff + rb.filled
	return offset >= rb.startOff && offset+size <= bufferEnd
}

// Stats returns buffer statistics
func (rb *RingBuffer) Stats() (startOff, filled, capacity int64) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.startOff, rb.filled, rb.size
}
