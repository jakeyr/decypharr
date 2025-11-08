package lightning

import (
	"sync"
	"time"
)

// ReadRouter decides between hot path (streaming) and cold path (caching)
// based on access pattern detection
type ReadRouter struct {
	mu      sync.RWMutex
	history map[string]*ReadHistory // fileID -> history

	// Configuration
	sequentialThreshold int64   // Bytes to consider sequential
	hotPathMinSize      int64   // Minimum size for hot path
	confidenceThreshold float64 // Min confidence for hot path
}

// ReadHistory tracks read patterns for a file
type ReadHistory struct {
	fileID string
	reads  []ReadRecord
	mu     sync.RWMutex

	// Cached pattern analysis
	lastAnalysis     time.Time
	cachedPattern    AccessPattern
	cachedConfidence float64
}

// ReadRecord represents a single read operation
type ReadRecord struct {
	offset    int64
	size      int64
	timestamp time.Time
}

// AccessPattern represents detected access pattern
type AccessPattern int

const (
	PatternUnknown AccessPattern = iota
	PatternSequential
	PatternRandom
	PatternStrided
	PatternReverse
)

func (p AccessPattern) String() string {
	switch p {
	case PatternSequential:
		return "sequential"
	case PatternRandom:
		return "random"
	case PatternStrided:
		return "strided"
	case PatternReverse:
		return "reverse"
	default:
		return "unknown"
	}
}

const (
	// Default configuration
	defaultSequentialThreshold = 128 * 1024      // 128KB gap tolerance
	defaultHotPathMinSize      = 4 * 1024 * 1024 // 4MB minimum for hot path
	defaultConfidenceThreshold = 0.85            // 85% confidence
	maxHistorySize             = 64              // Keep last 64 reads
	analysisInterval           = 5 * time.Second // Re-analyze every 5s
)

// NewReadRouter creates a new read router
func NewReadRouter() *ReadRouter {
	return &ReadRouter{
		history:             make(map[string]*ReadHistory),
		sequentialThreshold: defaultSequentialThreshold,
		hotPathMinSize:      defaultHotPathMinSize,
		confidenceThreshold: defaultConfidenceThreshold,
	}
}

// ShouldUseHotPath determines if a read should use the hot path (streaming)
// or cold path (chunk cache)
func (rr *ReadRouter) ShouldUseHotPath(fileID string, offset, size int64) bool {
	history := rr.getOrCreateHistory(fileID)
	history.recordRead(offset, size)

	pattern, confidence := history.analyzePattern()

	// Decision logic:
	// 1. Sequential reads with high confidence -> Hot path
	// 2. Large reads (>4MB) with any sequential pattern -> Hot path
	// 3. Random or strided -> Cold path

	switch pattern {
	case PatternSequential, PatternReverse:
		// Sequential access is great for streaming
		if confidence >= rr.confidenceThreshold {
			return true
		}
		// Even with lower confidence, large reads benefit from streaming
		if size >= rr.hotPathMinSize {
			return true
		}

	case PatternStrided:
		// Strided might benefit from streaming if stride is small
		// and reads are large
		if size >= rr.hotPathMinSize && confidence >= 0.7 {
			return true
		}

	case PatternRandom:
		// Random access should use cache
		return false

	case PatternUnknown:
		// Not enough data, default to hot path for large reads
		if size >= rr.hotPathMinSize {
			return true
		}
	}

	return false
}

// GetPattern returns the current pattern and confidence for a file
func (rr *ReadRouter) GetPattern(fileID string) (AccessPattern, float64) {
	history := rr.getOrCreateHistory(fileID)
	return history.analyzePattern()
}

// getOrCreateHistory gets or creates history for a file
func (rr *ReadRouter) getOrCreateHistory(fileID string) *ReadHistory {
	rr.mu.RLock()
	history, exists := rr.history[fileID]
	rr.mu.RUnlock()

	if exists {
		return history
	}

	rr.mu.Lock()
	defer rr.mu.Unlock()

	// Double-check after acquiring write lock
	if history, exists = rr.history[fileID]; exists {
		return history
	}

	history = &ReadHistory{
		fileID: fileID,
		reads:  make([]ReadRecord, 0, maxHistorySize),
	}
	rr.history[fileID] = history
	return history
}

// recordRead adds a read to the history
func (rh *ReadHistory) recordRead(offset, size int64) {
	rh.mu.Lock()
	defer rh.mu.Unlock()

	record := ReadRecord{
		offset:    offset,
		size:      size,
		timestamp: time.Now(),
	}

	rh.reads = append(rh.reads, record)

	// Keep only recent reads
	if len(rh.reads) > maxHistorySize {
		rh.reads = rh.reads[len(rh.reads)-maxHistorySize:]
	}
}

// analyzePattern analyzes the read pattern
func (rh *ReadHistory) analyzePattern() (AccessPattern, float64) {
	rh.mu.RLock()
	defer rh.mu.RUnlock()

	// Use cached analysis if recent enough
	if time.Since(rh.lastAnalysis) < analysisInterval && rh.cachedPattern != PatternUnknown {
		return rh.cachedPattern, rh.cachedConfidence
	}

	// Need at least 3 reads to detect pattern
	if len(rh.reads) < 3 {
		return PatternUnknown, 0.0
	}

	// Analyze pattern
	pattern, confidence := rh.detectPattern()

	// Cache result
	rh.lastAnalysis = time.Now()
	rh.cachedPattern = pattern
	rh.cachedConfidence = confidence

	return pattern, confidence
}

// detectPattern performs the actual pattern detection
func (rh *ReadHistory) detectPattern() (AccessPattern, float64) {
	if len(rh.reads) < 2 {
		return PatternUnknown, 0.0
	}

	// Analyze gaps between consecutive reads
	gaps := make([]int64, 0, len(rh.reads)-1)
	for i := 1; i < len(rh.reads); i++ {
		prev := rh.reads[i-1]
		curr := rh.reads[i]

		// Gap is the difference between current offset and where previous read ended
		expectedNext := prev.offset + prev.size
		gap := curr.offset - expectedNext

		gaps = append(gaps, gap)
	}

	// Calculate statistics
	sequentialCount := 0
	reverseCount := 0
	stridedCount := 0
	randomCount := 0

	var strideSizes []int64
	const gapTolerance = defaultSequentialThreshold

	for _, gap := range gaps {
		switch {
		case gap >= -gapTolerance && gap <= gapTolerance:
			// Sequential (allowing small gaps or overlaps)
			sequentialCount++
		case gap < -gapTolerance:
			// Reverse (reading backwards)
			reverseCount++
		default:
			// Either strided or random
			strideSizes = append(strideSizes, gap)
		}
	}

	// Check if strides are consistent
	if len(strideSizes) > 0 {
		stridedCount, randomCount = analyzeStrides(strideSizes)
	}

	// Determine pattern based on counts
	total := len(gaps)
	if total == 0 {
		return PatternUnknown, 0.0
	}

	// Calculate percentages
	seqPct := float64(sequentialCount) / float64(total)
	revPct := float64(reverseCount) / float64(total)
	stridePct := float64(stridedCount) / float64(total)
	randomPct := float64(randomCount) / float64(total)

	// Determine dominant pattern
	switch {
	case seqPct > 0.7:
		return PatternSequential, seqPct

	case revPct > 0.7:
		return PatternReverse, revPct

	case stridePct > 0.6:
		return PatternStrided, stridePct

	case randomPct > 0.5 || (seqPct < 0.5 && stridePct < 0.5):
		return PatternRandom, randomPct

	default:
		// Mixed pattern - if sequential + strided > 60%, treat as sequential
		if seqPct+stridePct > 0.6 {
			return PatternSequential, seqPct + stridePct
		}
		return PatternRandom, 1.0 - (seqPct + stridePct)
	}
}

// analyzeStrides checks if stride sizes are consistent
func analyzeStrides(strides []int64) (stridedCount, randomCount int) {
	if len(strides) == 0 {
		return 0, 0
	}

	// Group strides by similarity (within 20% tolerance)
	strideGroups := make(map[int64]int)
	const strideTolerance = 0.2

	for _, stride := range strides {
		// Find matching group
		found := false
		for groupStride, count := range strideGroups {
			diff := abs(stride - groupStride)
			tolerance := int64(float64(groupStride) * strideTolerance)
			if diff <= tolerance {
				strideGroups[groupStride] = count + 1
				found = true
				break
			}
		}

		if !found {
			strideGroups[stride] = 1
		}
	}

	// Find largest group
	maxGroupSize := 0
	for _, count := range strideGroups {
		if count > maxGroupSize {
			maxGroupSize = count
		}
	}

	// If largest group is >60% of strides, it's strided
	threshold := int(float64(len(strides)) * 0.6)
	if maxGroupSize >= threshold {
		stridedCount = maxGroupSize
		randomCount = len(strides) - maxGroupSize
	} else {
		// Too much variance - random
		randomCount = len(strides)
	}

	return stridedCount, randomCount
}

// abs returns absolute value
func abs(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

// ClearHistory removes history for a file
func (rr *ReadRouter) ClearHistory(fileID string) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	delete(rr.history, fileID)
}

// GetStats returns statistics about the router
func (rr *ReadRouter) GetStats() map[string]interface{} {
	rr.mu.RLock()
	defer rr.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["tracked_files"] = len(rr.history)

	// Aggregate pattern distribution
	patterns := make(map[string]int)
	for _, history := range rr.history {
		pattern, _ := history.analyzePattern()
		patterns[pattern.String()]++
	}
	stats["patterns"] = patterns

	return stats
}

// SetConfiguration allows customizing router behavior
func (rr *ReadRouter) SetConfiguration(sequentialThreshold, hotPathMinSize int64, confidenceThreshold float64) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if sequentialThreshold > 0 {
		rr.sequentialThreshold = sequentialThreshold
	}
	if hotPathMinSize > 0 {
		rr.hotPathMinSize = hotPathMinSize
	}
	if confidenceThreshold > 0 && confidenceThreshold <= 1.0 {
		rr.confidenceThreshold = confidenceThreshold
	}
}
