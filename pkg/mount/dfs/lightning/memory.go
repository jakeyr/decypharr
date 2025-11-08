package lightning

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryBudget manages three-tier memory allocation for Lightning
type MemoryBudget struct {
	// Budget allocations
	L1_HotBuffers   int64 // Ring buffers for hot path
	L2_MemCache     int64 // LRU chunk cache
	L3_ReadaheadBuf int64 // Adaptive readahead window

	// Current usage
	l1Used atomic.Int64
	l2Used atomic.Int64
	l3Used atomic.Int64

	// Configuration
	totalBudget     int64
	minL1           int64
	minL2           int64
	minL3           int64
	pressureMonitor *MemoryPressureMonitor

	mu sync.RWMutex
}

// MemoryPressureMonitor monitors system memory pressure
type MemoryPressureMonitor struct {
	ctx                context.Context
	cancel             context.CancelFunc
	budget             *MemoryBudget
	checkInterval      time.Duration
	systemMemThreshold float64 // Percentage of system memory (0.8 = 80%)

	// Metrics
	currentPressure atomic.Int64 // 0-100 percentage
	systemMemTotal  uint64
	wg              sync.WaitGroup
}

const (
	defaultL1Size = 64 * 1024 * 1024  // 64MB - Hot buffers
	defaultL2Size = 256 * 1024 * 1024 // 256MB - Memory cache
	defaultL3Size = 128 * 1024 * 1024 // 128MB - Readahead

	defaultTotalBudget = 448 * 1024 * 1024 // 448MB total

	// Minimum allocations (can shrink to these under pressure)
	minL1Size = 16 * 1024 * 1024 // 16MB minimum
	minL2Size = 64 * 1024 * 1024 // 64MB minimum
	minL3Size = 0                // Can disable readahead

	// System memory threshold
	defaultSysMemThreshold = 0.8 // Don't use more than 80% of system memory
	checkInterval          = 10 * time.Second
)

// NewMemoryBudget creates a new memory budget manager
func NewMemoryBudget(totalBudget int64) *MemoryBudget {
	if totalBudget <= 0 {
		totalBudget = defaultTotalBudget
	}

	mb := &MemoryBudget{
		totalBudget: totalBudget,
		minL1:       minL1Size,
		minL2:       minL2Size,
		minL3:       minL3Size,
	}

	// Allocate budget across tiers
	mb.allocateBudget(totalBudget)

	// Create pressure monitor
	mb.pressureMonitor = newMemoryPressureMonitor(mb)
	mb.pressureMonitor.start()

	return mb
}

// allocateBudget distributes total budget across tiers
func (mb *MemoryBudget) allocateBudget(total int64) {
	// Default ratios: L1=14%, L2=57%, L3=29%
	mb.L1_HotBuffers = total * 14 / 100
	mb.L2_MemCache = total * 57 / 100
	mb.L3_ReadaheadBuf = total * 29 / 100

	// Ensure minimums
	if mb.L1_HotBuffers < mb.minL1 {
		mb.L1_HotBuffers = mb.minL1
	}
	if mb.L2_MemCache < mb.minL2 {
		mb.L2_MemCache = mb.minL2
	}
	if mb.L3_ReadaheadBuf < mb.minL3 {
		mb.L3_ReadaheadBuf = mb.minL3
	}
}

// AllocateL1 allocates from L1 tier (hot buffers)
func (mb *MemoryBudget) AllocateL1(size int64) bool {
	current := mb.l1Used.Load()
	if current+size > mb.L1_HotBuffers {
		return false
	}

	mb.l1Used.Add(size)
	return true
}

// FreeL1 frees L1 allocation
func (mb *MemoryBudget) FreeL1(size int64) {
	mb.l1Used.Add(-size)
}

// AllocateL2 allocates from L2 tier (memory cache)
func (mb *MemoryBudget) AllocateL2(size int64) bool {
	current := mb.l2Used.Load()
	if current+size > mb.L2_MemCache {
		return false
	}

	mb.l2Used.Add(size)
	return true
}

// FreeL2 frees L2 allocation
func (mb *MemoryBudget) FreeL2(size int64) {
	mb.l2Used.Add(-size)
}

// AllocateL3 allocates from L3 tier (readahead)
func (mb *MemoryBudget) AllocateL3(size int64) bool {
	current := mb.l3Used.Load()
	if current+size > mb.L3_ReadaheadBuf {
		return false
	}

	mb.l3Used.Add(size)
	return true
}

// FreeL3 frees L3 allocation
func (mb *MemoryBudget) FreeL3(size int64) {
	mb.l3Used.Add(-size)
}

// GetPressure returns current memory pressure (0-100)
func (mb *MemoryBudget) GetPressure() int {
	return int(mb.pressureMonitor.currentPressure.Load())
}

// AdjustForPressure adjusts budgets based on memory pressure
func (mb *MemoryBudget) AdjustForPressure(pressure int) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	switch {
	case pressure > 90:
		// Critical pressure - shrink to minimums
		mb.L1_HotBuffers = mb.minL1
		mb.L2_MemCache = mb.minL2
		mb.L3_ReadaheadBuf = 0 // Disable readahead

	case pressure > 70:
		// High pressure - reduce by 50%
		mb.L1_HotBuffers = max(mb.minL1, mb.L1_HotBuffers/2)
		mb.L2_MemCache = max(mb.minL2, mb.L2_MemCache/2)
		mb.L3_ReadaheadBuf = max(mb.minL3, mb.L3_ReadaheadBuf/2)

	case pressure > 50:
		// Medium pressure - reduce by 25%
		mb.L1_HotBuffers = max(mb.minL1, mb.L1_HotBuffers*3/4)
		mb.L2_MemCache = max(mb.minL2, mb.L2_MemCache*3/4)
		mb.L3_ReadaheadBuf = max(mb.minL3, mb.L3_ReadaheadBuf*3/4)

	case pressure < 30:
		// Low pressure - restore to defaults
		mb.allocateBudget(mb.totalBudget)
	}
}

// GetStats returns memory budget statistics
func (mb *MemoryBudget) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"total_budget_mb": mb.totalBudget / (1024 * 1024),
		"l1_budget_mb":    mb.L1_HotBuffers / (1024 * 1024),
		"l2_budget_mb":    mb.L2_MemCache / (1024 * 1024),
		"l3_budget_mb":    mb.L3_ReadaheadBuf / (1024 * 1024),
		"l1_used_mb":      mb.l1Used.Load() / (1024 * 1024),
		"l2_used_mb":      mb.l2Used.Load() / (1024 * 1024),
		"l3_used_mb":      mb.l3Used.Load() / (1024 * 1024),
		"l1_usage_pct":    float64(mb.l1Used.Load()) / float64(mb.L1_HotBuffers) * 100,
		"l2_usage_pct":    float64(mb.l2Used.Load()) / float64(mb.L2_MemCache) * 100,
		"l3_usage_pct":    float64(mb.l3Used.Load()) / float64(max(1, mb.L3_ReadaheadBuf)) * 100,
		"pressure_pct":    mb.GetPressure(),
	}
}

// newMemoryPressureMonitor creates a memory pressure monitor
func newMemoryPressureMonitor(budget *MemoryBudget) *MemoryPressureMonitor {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return &MemoryPressureMonitor{
		budget:             budget,
		checkInterval:      checkInterval,
		systemMemThreshold: defaultSysMemThreshold,
		systemMemTotal:     m.Sys,
	}
}

// start begins monitoring
func (mpm *MemoryPressureMonitor) start() {
	mpm.wg.Add(1)
	go mpm.monitorLoop()
}

// monitorLoop periodically checks memory pressure
func (mpm *MemoryPressureMonitor) monitorLoop() {
	defer mpm.wg.Done()

	ticker := time.NewTicker(mpm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mpm.checkPressure()
		case <-mpm.ctx.Done():
			return
		}
	}
}

// checkPressure calculates current memory pressure
func (mpm *MemoryPressureMonitor) checkPressure() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Calculate pressure based on:
	// 1. Lightning memory usage vs budget
	// 2. System memory usage

	// Lightning pressure
	lightningUsed := mpm.budget.l1Used.Load() +
		mpm.budget.l2Used.Load() +
		mpm.budget.l3Used.Load()
	lightningBudget := mpm.budget.totalBudget

	lightningPressure := 0
	if lightningBudget > 0 {
		lightningPressure = int(float64(lightningUsed) / float64(lightningBudget) * 100)
	}

	// System pressure (Go heap usage)
	systemPressure := 0
	if mpm.systemMemTotal > 0 {
		systemPressure = int(float64(m.Alloc) / float64(mpm.systemMemTotal) * 100)
	}

	// Overall pressure is max of both
	pressure := lightningPressure
	if systemPressure > pressure {
		pressure = systemPressure
	}
	if pressure > 100 {
		pressure = 100
	}

	mpm.currentPressure.Store(int64(pressure))

	// Adjust budgets if needed
	if pressure > 50 {
		mpm.budget.AdjustForPressure(pressure)
	}
}

// Stop stops the pressure monitor
func (mpm *MemoryPressureMonitor) Stop() {
	if mpm.cancel != nil {
		mpm.cancel()
		mpm.wg.Wait()
	}
}

// SetTotalBudget updates the total memory budget
func (mb *MemoryBudget) SetTotalBudget(budget int64) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	mb.totalBudget = budget
	mb.allocateBudget(budget)
}

// SetTierMinimums sets minimum sizes for each tier
func (mb *MemoryBudget) SetTierMinimums(l1, l2, l3 int64) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if l1 > 0 {
		mb.minL1 = l1
	}
	if l2 > 0 {
		mb.minL2 = l2
	}
	if l3 >= 0 {
		mb.minL3 = l3
	}
}

// GetAvailableL1 returns available L1 budget
func (mb *MemoryBudget) GetAvailableL1() int64 {
	return mb.L1_HotBuffers - mb.l1Used.Load()
}

// GetAvailableL2 returns available L2 budget
func (mb *MemoryBudget) GetAvailableL2() int64 {
	return mb.L2_MemCache - mb.l2Used.Load()
}

// GetAvailableL3 returns available L3 budget
func (mb *MemoryBudget) GetAvailableL3() int64 {
	return mb.L3_ReadaheadBuf - mb.l3Used.Load()
}

// Close stops the memory manager
func (mb *MemoryBudget) Close() {
	if mb.pressureMonitor != nil {
		mb.pressureMonitor.Stop()
	}
}

// ForceGC triggers garbage collection
func (mb *MemoryBudget) ForceGC() {
	runtime.GC()
}

// GetMemoryStats returns detailed memory statistics
func (mb *MemoryBudget) GetMemoryStats() map[string]interface{} {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	stats := mb.GetStats()
	stats["go_alloc_mb"] = m.Alloc / (1024 * 1024)
	stats["go_sys_mb"] = m.Sys / (1024 * 1024)
	stats["go_heap_alloc_mb"] = m.HeapAlloc / (1024 * 1024)
	stats["go_heap_sys_mb"] = m.HeapSys / (1024 * 1024)
	stats["go_num_gc"] = m.NumGC

	return stats
}
