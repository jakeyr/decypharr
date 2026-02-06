package vfs

import "sync/atomic"

// Stats tracks VFS-wide statistics for debugging and monitoring
type Stats struct {
	// Download stats
	TotalChunks atomic.Int64 // Total chunks downloaded
	TotalBytes  atomic.Int64 // Total bytes downloaded
	CacheHits   atomic.Int64 // Reads served from cache
	CacheMisses atomic.Int64 // Reads that required download
	StreamCalls atomic.Int64 // Calls to manager.Stream

	// Error stats
	DownloadErrors atomic.Int64 // Total download errors
	RetryAttempts  atomic.Int64 // Total retry attempts
	CircuitTrips   atomic.Int64 // Times circuit breaker tripped
	CircuitRejects atomic.Int64 // Requests rejected due to open circuit

	// Downloader stats
	DownloadersCreated atomic.Int64 // Total downloaders created
	DownloadersReused  atomic.Int64 // Times existing downloader was reused

	// File handle stats
	FilesOpened atomic.Int64 // Number of file handles opened via VFS
	FilesClosed atomic.Int64 // Number of file handles closed via VFS
}

// GlobalVFSStats is the global stats instance
var GlobalVFSStats = &Stats{}

// ToMap converts stats to a map for JSON serialization
func (s *Stats) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"total_chunks":        s.TotalChunks.Load(),
		"total_bytes":         s.TotalBytes.Load(),
		"cache_hits":          s.CacheHits.Load(),
		"cache_misses":        s.CacheMisses.Load(),
		"stream_calls":        s.StreamCalls.Load(),
		"download_errors":     s.DownloadErrors.Load(),
		"retry_attempts":      s.RetryAttempts.Load(),
		"circuit_trips":       s.CircuitTrips.Load(),
		"circuit_rejects":     s.CircuitRejects.Load(),
		"downloaders_created": s.DownloadersCreated.Load(),
		"downloaders_reused":  s.DownloadersReused.Load(),
		"files_opened":        s.FilesOpened.Load(),
		"files_closed":        s.FilesClosed.Load(),
	}
}

// Reset resets all stats to zero
func (s *Stats) Reset() {
	s.TotalChunks.Store(0)
	s.TotalBytes.Store(0)
	s.CacheHits.Store(0)
	s.CacheMisses.Store(0)
	s.StreamCalls.Store(0)
	s.DownloadErrors.Store(0)
	s.RetryAttempts.Store(0)
	s.CircuitTrips.Store(0)
	s.CircuitRejects.Store(0)
	s.DownloadersCreated.Store(0)
	s.DownloadersReused.Store(0)
	s.FilesOpened.Store(0)
	s.FilesClosed.Store(0)
}
