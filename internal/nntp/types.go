package nntp

import (
	"sync/atomic"
	"time"
)

// Segment represents a usenet segment
type Segment struct {
	MessageID string
	Number    int
	Bytes     int64
	Data      []byte
}

// Article represents a complete usenet article
type Article struct {
	MessageID string
	Subject   string
	From      string
	Date      string
	Groups    []string
	Body      []byte
	Size      int64
}

// Response represents an NNTP server response
type Response struct {
	Code    int
	Message string
	Lines   []string
}

// GroupInfo represents information about a newsgroup
type GroupInfo struct {
	Name  string
	Count int // Number of articles in the group
	Low   int // Lowest article number
	High  int // Highest article number
}

// StatResult represents the result of a STAT command for a single message ID
type StatResult struct {
	MessageID string // The message ID that was checked
	Available bool   // Whether the article is available
	Error     error  // Error if any (nil means success or article found)
}

// BatchStatResult contains results for all message IDs in a batch
type BatchStatResult struct {
	Results    []StatResult // Per-message results
	TotalCount int          // Total number of messages checked
	FoundCount int          // Number of messages found
	ErrorCount int          // Number of errors (excluding not found)
}

// HasErrors returns true if any non-ArticleNotFound errors occurred
func (r *BatchStatResult) HasErrors() bool {
	return r.ErrorCount > 0
}

// AllAvailable returns true if all messages are available
func (r *BatchStatResult) AllAvailable() bool {
	return r.FoundCount == r.TotalCount
}

// FirstError returns the first error encountered, or nil if none
func (r *BatchStatResult) FirstError() error {
	for _, res := range r.Results {
		if res.Error != nil {
			return res.Error
		}
	}
	return nil
}

// ProviderMetrics tracks performance metrics for a single provider
type ProviderMetrics struct {
	// Latency tracking (exponential moving average)
	AvgLatencyMs atomic.Int64 // Average latency in milliseconds

	// Error tracking
	TotalRequests  atomic.Int64 // Total requests made
	TotalErrors    atomic.Int64 // Total errors (all types)
	ConnectionErrs atomic.Int64 // Connection-level errors
	ArticleNotFound atomic.Int64 // Article not found errors (not really errors)

	// Throughput
	BytesDownloaded atomic.Int64 // Total bytes downloaded

	// Timestamps
	LastSuccess atomic.Int64 // Unix timestamp of last successful request
	LastError   atomic.Int64 // Unix timestamp of last error
}

// RecordSuccess records a successful request
func (m *ProviderMetrics) RecordSuccess(latencyMs int64, bytesRead int64) {
	m.TotalRequests.Add(1)
	m.BytesDownloaded.Add(bytesRead)
	m.LastSuccess.Store(time.Now().Unix())

	// Update exponential moving average for latency (alpha = 0.2)
	// newAvg = alpha * newValue + (1 - alpha) * oldAvg
	oldAvg := m.AvgLatencyMs.Load()
	if oldAvg == 0 {
		m.AvgLatencyMs.Store(latencyMs)
	} else {
		newAvg := (latencyMs + 4*oldAvg) / 5 // alpha = 0.2
		m.AvgLatencyMs.Store(newAvg)
	}
}

// RecordError records an error
func (m *ProviderMetrics) RecordError(err error) {
	m.TotalRequests.Add(1)
	m.TotalErrors.Add(1)
	m.LastError.Store(time.Now().Unix())

	// Classify error type
	var nntpErr *Error
	if err != nil {
		if asErr, ok := err.(*Error); ok {
			nntpErr = asErr
		}
	}

	if nntpErr != nil {
		switch nntpErr.Type {
		case ErrorTypeConnection:
			m.ConnectionErrs.Add(1)
		case ErrorTypeArticleNotFound:
			m.ArticleNotFound.Add(1)
			// Don't count article not found as a real error
			m.TotalErrors.Add(-1)
		}
	} else {
		m.ConnectionErrs.Add(1)
	}
}

// ErrorRate returns the error rate (0.0 to 1.0), excluding article not found
func (m *ProviderMetrics) ErrorRate() float64 {
	total := m.TotalRequests.Load()
	if total == 0 {
		return 0
	}
	errors := m.TotalErrors.Load()
	return float64(errors) / float64(total)
}

// GetStats returns metrics as a map for JSON serialization
func (m *ProviderMetrics) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"avg_latency_ms":    m.AvgLatencyMs.Load(),
		"total_requests":    m.TotalRequests.Load(),
		"total_errors":      m.TotalErrors.Load(),
		"connection_errors": m.ConnectionErrs.Load(),
		"article_not_found": m.ArticleNotFound.Load(),
		"bytes_downloaded":  m.BytesDownloaded.Load(),
		"error_rate":        m.ErrorRate(),
		"last_success":      m.LastSuccess.Load(),
		"last_error":        m.LastError.Load(),
	}
}
