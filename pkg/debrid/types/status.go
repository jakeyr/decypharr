package types

// TorrentStatus represents the current state of a managed torrent
type TorrentStatus string

const (
	TorrentStatusPending     TorrentStatus = "pending"     // Waiting to be processed
	TorrentStatusQueued      TorrentStatus = "queued"      // In import queue (too many active downloads)
	TorrentStatusSubmitting  TorrentStatus = "submitting"  // Being submitted to debrid
	TorrentStatusDownloading TorrentStatus = "downloading" // Downloading on debrid (in active bucket)
	TorrentStatusProcessing  TorrentStatus = "processing"  // Creating symlinks/downloading files
	TorrentStatusCompleted   TorrentStatus = "completed"   // Fully complete and ready (moved to cached bucket)
	TorrentStatusImported    TorrentStatus = "imported"    // Imported by Sonarr/Radarr (can be cleaned up)
	TorrentStatusError       TorrentStatus = "error"       // Failed
	TorrentStatusSwitching   TorrentStatus = "switching"   // Being moved between debrids
	TorrentStatusArchived    TorrentStatus = "archived"
)
