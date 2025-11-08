package lightning

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// AdaptiveReadahead manages dynamic readahead window sizing
// based on hit rate and system memory pressure
type AdaptiveReadahead struct {
	mu sync.RWMutex

	// Current state
	currentWindow atomic.Int64 // Current readahead window size
	minWindow     int64         // Minimum window (256KB)
	maxWindow     int64         // Maximum window (4MB)

	// Performance tracking
	totalPrefetch   atomic.Int64 // Total bytes prefetched
	usedPrefetch    atomic.Int64 // Bytes actually used
	wastedPrefetch  atomic.Int64 // Bytes prefetched but not used
	prefetchHits    atomic.Int64 // Successful prefetch predictions
	prefetchMisses  atomic.Int64 // Failed predictions
	lastAdjustment  time.Time
	adjustInterval  time.Duration

	// Memory pressure tracking
	memoryBudget    int64 // Total memory budget
	memoryUsed      atomic.Int64
	memoryThreshold float64 // Percentage threshold (0.9 = 90%)

	// Active prefetch operations
	activePrefetch map[string]*PrefetchOperation
	prefetchMu     sync.RWMutex
}

// PrefetchOperation represents an active prefetch
type PrefetchOperation struct {
	fileID      string
	offset      int64
	size        int64
	startTime   time.Time
	buffer      []byte
	used        atomic.Bool
	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
}

const (
	defaultMinWindow      = 256 * 1024       // 256KB
	defaultMaxWindow      = 4 * 1024 * 1024  // 4MB
	defaultAdjustInterval = 5 * time.Second  // Adjust every 5 seconds
	defaultMemoryBudget   = 128 * 1024 * 1024 // 128MB for readahead
	defaultMemThreshold   = 0.9              // 90% memory usage threshold
	maxConcurrentPrefetch = 16               // Maximum concurrent prefetch operations

	// Growth/shrink factors
	growthFactor = 2.0
	shrinkFactor = 0.5

	// Thresholds for adjustment
	highHitRate = 0.90  // 90% hit rate -> grow
	lowHitRate  = 0.50  // 50% hit rate -> shrink
)

// NewAdaptiveReadahead creates a new adaptive readahead engine
func NewAdaptiveReadahead(memoryBudget int64) *AdaptiveReadahead {
	if memoryBudget <= 0 {
		memoryBudget = defaultMemoryBudget
	}

	ar := &AdaptiveReadahead{
		minWindow:       defaultMinWindow,
		maxWindow:       defaultMaxWindow,
		adjustInterval:  defaultAdjustInterval,
		memoryBudget:    memoryBudget,
		memoryThreshold: defaultMemThreshold,
		lastAdjustment:  time.Now(),
		activePrefetch:  make(map[string]*PrefetchOperation),
	}

	// Start with minimum window
	ar.currentWindow.Store(defaultMinWindow)

	return ar
}

// GetWindow returns the current readahead window size
func (ar *AdaptiveReadahead) GetWindow() int64 {
	// Check if we need to adjust
	ar.maybeAdjustWindow()
	return ar.currentWindow.Load()
}

// RecordHit records a successful prefetch hit
func (ar *AdaptiveReadahead) RecordHit(bytes int64) {
	ar.prefetchHits.Add(1)
	ar.usedPrefetch.Add(bytes)
}

// RecordMiss records a prefetch miss
func (ar *AdaptiveReadahead) RecordMiss() {
	ar.prefetchMisses.Add(1)
}

// RecordWaste records wasted prefetch bytes
func (ar *AdaptiveReadahead) RecordWaste(bytes int64) {
	ar.wastedPrefetch.Add(bytes)
}

// GetHitRate returns the current prefetch hit rate
func (ar *AdaptiveReadahead) GetHitRate() float64 {
	hits := float64(ar.prefetchHits.Load())
	misses := float64(ar.prefetchMisses.Load())
	total := hits + misses

	if total == 0 {
		return 0.0
	}

	return hits / total
}

// GetMemoryPressure returns current memory pressure (0.0-1.0)
func (ar *AdaptiveReadahead) GetMemoryPressure() float64 {
	used := float64(ar.memoryUsed.Load())
	budget := float64(ar.memoryBudget)

	if budget == 0 {
		return 0.0
	}

	return used / budget
}

// maybeAdjustWindow checks if window should be adjusted
func (ar *AdaptiveReadahead) maybeAdjustWindow() {
	// Fast path: Check without lock (double-checked locking)
	ar.mu.RLock()
	needsAdjustment := time.Since(ar.lastAdjustment) >= ar.adjustInterval
	ar.mu.RUnlock()

	if !needsAdjustment {
		return // No lock acquisition needed - 99% case
	}

	// Slow path: Actually need to adjust
	ar.mu.Lock()
	defer ar.mu.Unlock()

	// Double-check after acquiring write lock (another goroutine might have adjusted)
	if time.Since(ar.lastAdjustment) < ar.adjustInterval {
		return
	}

	ar.lastAdjustment = time.Now()

	// Get current metrics
	hitRate := ar.GetHitRate()
	memPressure := ar.GetMemoryPressure()
	currentWindow := ar.currentWindow.Load()

	// Decision logic (inspired by Linux kernel readahead)
	var newWindow int64

	switch {
	case memPressure > ar.memoryThreshold:
		// High memory pressure - shrink aggressively
		newWindow = int64(float64(currentWindow) * shrinkFactor)

	case hitRate > highHitRate && memPressure < 0.6:
		// High hit rate and memory available - grow
		newWindow = int64(float64(currentWindow) * growthFactor)

	case hitRate < lowHitRate:
		// Low hit rate - shrink
		newWindow = int64(float64(currentWindow) * shrinkFactor)

	default:
		// Stable - no change
		newWindow = currentWindow
	}

	// Clamp to min/max
	if newWindow < ar.minWindow {
		newWindow = ar.minWindow
	}
	if newWindow > ar.maxWindow {
		newWindow = ar.maxWindow
	}

	// Apply new window
	if newWindow != currentWindow {
		ar.currentWindow.Store(newWindow)
	}
}

// ShouldPrefetch determines if prefetching should be done
func (ar *AdaptiveReadahead) ShouldPrefetch() bool {
	// Don't prefetch if memory pressure is too high
	if ar.GetMemoryPressure() > ar.memoryThreshold {
		return false
	}

	// Don't prefetch if hit rate is very low
	hitRate := ar.GetHitRate()
	if hitRate > 0 && hitRate < 0.2 { // Less than 20% hit rate
		return false
	}

	return true
}

// StartPrefetch starts a prefetch operation
func (ar *AdaptiveReadahead) StartPrefetch(ctx context.Context, fileID string, offset int64, downloader func(context.Context, int64, int64) ([]byte, error)) *PrefetchOperation {
	// Check if we should prefetch
	if !ar.ShouldPrefetch() {
		return nil
	}

	window := ar.GetWindow()

	ar.prefetchMu.Lock()

	// CRITICAL: Check concurrent prefetch limit BEFORE reserving memory
	if len(ar.activePrefetch) >= maxConcurrentPrefetch {
		ar.prefetchMu.Unlock()
		ar.RecordMiss() // Track that we skipped
		return nil
	}

	// Check if we have memory budget
	if ar.memoryUsed.Load()+window > ar.memoryBudget {
		ar.prefetchMu.Unlock()
		return nil
	}

	// Check if already prefetching this range
	key := prefetchKey(fileID, offset)
	if _, exists := ar.activePrefetch[key]; exists {
		ar.prefetchMu.Unlock()
		return nil
	}

	// Create operation
	ctx, cancel := context.WithCancel(ctx)
	op := &PrefetchOperation{
		fileID:    fileID,
		offset:    offset,
		size:      window,
		startTime: time.Now(),
		ctx:       ctx,
		cancel:    cancel,
	}

	// Reserve memory
	ar.memoryUsed.Add(window)
	ar.totalPrefetch.Add(window)

	// Store operation
	ar.activePrefetch[key] = op
	ar.prefetchMu.Unlock()

	// Start background prefetch
	go ar.executePrefetch(op, downloader)

	return op
}

// executePrefetch performs the actual prefetch
func (ar *AdaptiveReadahead) executePrefetch(op *PrefetchOperation, downloader func(context.Context, int64, int64) ([]byte, error)) {
	defer func() {
		// Clean up on completion
		key := prefetchKey(op.fileID, op.offset)
		ar.prefetchMu.Lock()
		delete(ar.activePrefetch, key)
		ar.prefetchMu.Unlock()

		// Release memory if not used
		if !op.used.Load() {
			ar.memoryUsed.Add(-op.size)
			ar.RecordWaste(op.size)
		}
	}()

	// Download data
	buffer, err := downloader(op.ctx, op.offset, op.size)
	if err != nil {
		// Prefetch failed
		ar.RecordMiss()
		return
	}

	// Store buffer
	op.mu.Lock()
	op.buffer = buffer
	op.mu.Unlock()
}

// GetPrefetch tries to retrieve prefetched data
func (ar *AdaptiveReadahead) GetPrefetch(fileID string, offset int64, size int64) []byte {
	key := prefetchKey(fileID, offset)

	ar.prefetchMu.RLock()
	op, exists := ar.activePrefetch[key]
	ar.prefetchMu.RUnlock()

	if !exists {
		ar.RecordMiss()
		return nil
	}

	op.mu.RLock()
	defer op.mu.RUnlock()

	// Check if data is ready and covers requested range
	if op.buffer == nil {
		ar.RecordMiss()
		return nil
	}

	if size > int64(len(op.buffer)) {
		// Partial hit
		ar.RecordHit(int64(len(op.buffer)))
		op.used.Store(true)
		return op.buffer
	}

	// Full hit
	ar.RecordHit(size)
	op.used.Store(true)
	return op.buffer[:size]
}

// CancelPrefetch cancels an active prefetch
func (ar *AdaptiveReadahead) CancelPrefetch(fileID string, offset int64) {
	key := prefetchKey(fileID, offset)

	ar.prefetchMu.Lock()
	defer ar.prefetchMu.Unlock()

	if op, exists := ar.activePrefetch[key]; exists {
		op.cancel()
		delete(ar.activePrefetch, key)
	}
}

// prefetchKey generates a unique key for a prefetch operation
func prefetchKey(fileID string, offset int64) string {
	// Pre-allocate buffer to avoid allocations
	buf := make([]byte, 0, len(fileID)+20)
	buf = append(buf, fileID...)
	buf = append(buf, ':')
	// Use strconv for efficient integer conversion
	buf = strconv.AppendInt(buf, offset, 10)
	return string(buf)
}

// GetStats returns readahead statistics
func (ar *AdaptiveReadahead) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"current_window_kb":   ar.currentWindow.Load() / 1024,
		"min_window_kb":       ar.minWindow / 1024,
		"max_window_kb":       ar.maxWindow / 1024,
		"hit_rate":            ar.GetHitRate(),
		"memory_pressure":     ar.GetMemoryPressure(),
		"memory_used_mb":      ar.memoryUsed.Load() / (1024 * 1024),
		"memory_budget_mb":    ar.memoryBudget / (1024 * 1024),
		"total_prefetch_mb":   ar.totalPrefetch.Load() / (1024 * 1024),
		"used_prefetch_mb":    ar.usedPrefetch.Load() / (1024 * 1024),
		"wasted_prefetch_mb":  ar.wastedPrefetch.Load() / (1024 * 1024),
		"prefetch_hits":       ar.prefetchHits.Load(),
		"prefetch_misses":     ar.prefetchMisses.Load(),
		"active_prefetches":   len(ar.activePrefetch),
	}
}

// SetWindowLimits allows customizing window size limits
func (ar *AdaptiveReadahead) SetWindowLimits(minWindow, maxWindow int64) {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	if minWindow > 0 {
		ar.minWindow = minWindow
	}
	if maxWindow > 0 && maxWindow >= ar.minWindow {
		ar.maxWindow = maxWindow
	}

	// Ensure current window is within bounds
	current := ar.currentWindow.Load()
	if current < ar.minWindow {
		ar.currentWindow.Store(ar.minWindow)
	}
	if current > ar.maxWindow {
		ar.currentWindow.Store(ar.maxWindow)
	}
}

// SetMemoryBudget updates the memory budget
func (ar *AdaptiveReadahead) SetMemoryBudget(budget int64) {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	if budget > 0 {
		ar.memoryBudget = budget
	}
}

// Reset resets all statistics
func (ar *AdaptiveReadahead) Reset() {
	ar.prefetchHits.Store(0)
	ar.prefetchMisses.Store(0)
	ar.totalPrefetch.Store(0)
	ar.usedPrefetch.Store(0)
	ar.wastedPrefetch.Store(0)
	ar.currentWindow.Store(ar.minWindow)

	// Cancel all active prefetches
	ar.prefetchMu.Lock()
	for _, op := range ar.activePrefetch {
		op.cancel()
	}
	ar.activePrefetch = make(map[string]*PrefetchOperation)
	ar.prefetchMu.Unlock()

	ar.memoryUsed.Store(0)
}
