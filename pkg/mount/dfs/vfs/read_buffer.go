package vfs

import "sync"

// tailReadBuffer keeps a bounded in-memory window of recently written bytes.
// It is optimized for sequential streaming reads and intentionally simple:
// out-of-order writes may reset the window, but memory usage remains bounded.
type tailReadBuffer struct {
	mu    sync.RWMutex
	start int64
	data  []byte
	cap   int
}

func newTailReadBuffer(maxBytes int64) *tailReadBuffer {
	if maxBytes <= 0 {
		return nil
	}
	if maxBytes > 32*1024*1024 {
		maxBytes = 32 * 1024 * 1024
	}
	return &tailReadBuffer{
		cap: int(maxBytes),
	}
}

func (b *tailReadBuffer) Clear() {
	b.mu.Lock()
	b.start = 0
	b.data = nil
	b.mu.Unlock()
}

func (b *tailReadBuffer) ReadAt(p []byte, off int64) (int, bool) {
	if len(p) == 0 {
		return 0, true
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.data) == 0 {
		return 0, false
	}

	end := b.start + int64(len(b.data))
	readEnd := off + int64(len(p))
	if off < b.start || readEnd > end {
		return 0, false
	}

	startIdx := int(off - b.start)
	copy(p, b.data[startIdx:startIdx+len(p)])
	return len(p), true
}

func (b *tailReadBuffer) WriteAt(off int64, p []byte) {
	if len(p) == 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.cap <= 0 {
		return
	}

	if len(b.data) == 0 {
		b.resetWindowLocked(off, p)
		return
	}

	end := b.start + int64(len(b.data))
	writeEnd := off + int64(len(p))

	// Disjoint ranges reset the window to incoming data.
	if off > end || writeEnd < b.start {
		b.resetWindowLocked(off, p)
		return
	}

	// Older write starts before the current window start: reset for simplicity.
	if off < b.start {
		b.resetWindowLocked(off, p)
		return
	}

	rel := int(off - b.start)
	if rel < len(b.data) {
		n := copy(b.data[rel:], p)
		if n < len(p) {
			b.data = append(b.data, p[n:]...)
		}
	} else {
		// Contiguous append.
		b.data = append(b.data, p...)
	}

	b.trimLocked()
}

func (b *tailReadBuffer) resetWindowLocked(off int64, p []byte) {
	if len(p) > b.cap {
		skip := len(p) - b.cap
		off += int64(skip)
		p = p[skip:]
	}
	b.start = off
	if cap(b.data) < len(p) {
		b.data = make([]byte, len(p))
	} else {
		b.data = b.data[:len(p)]
	}
	copy(b.data, p)
}

func (b *tailReadBuffer) trimLocked() {
	if len(b.data) <= b.cap {
		return
	}
	trim := len(b.data) - b.cap
	b.start += int64(trim)
	b.data = b.data[trim:]
}
