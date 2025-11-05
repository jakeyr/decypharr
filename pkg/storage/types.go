package storage

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

type SwitcherStatus string

const (
	SwitcherStatusPending    SwitcherStatus = "pending"
	SwitcherStatusInProgress SwitcherStatus = "in_progress"
	SwitcherStatusCompleted  SwitcherStatus = "completed"
	SwitcherStatusFailed     SwitcherStatus = "failed"
)

// Common errors
var (
	ErrPlacementNotFound     = fmt.Errorf("placement not found")
	ErrAlreadyOnDebrid       = fmt.Errorf("torrent already on this debrid")
	ErrPlacementNotCompleted = fmt.Errorf("placement not completed")
	ErrNoActivePlacement     = fmt.Errorf("no active placement")
)

// Torrent is the unified torrent model across all debrids
// This replaces both wire.Torrent and debrid cache CachedTorrent
type Torrent struct {
	// Core Identity (from debrid cache)
	InfoHash         string `msgpack:"info_hash" json:"info_hash"`                 // Primary key - torrent hash
	Name             string `msgpack:"name" json:"name"`                           // Torrent name
	Folder           string `msgpack:"folder" json:"folder"`                       // Folder name. This may differ from Name if config.WebDavFolderNaming is used
	OriginalFilename string `msgpack:"original_filename" json:"original_filename"` // Original filename from debrid
	Size             int64  `msgpack:"size" json:"size"`                           // Total size in bytes (for QBit compat)
	Bytes            int64  `msgpack:"bytes" json:"bytes"`                         // Actual bytes (debrid uses this)
	Magnet           string `msgpack:"magnet,omitempty" json:"magnet,omitempty"`   // Magnet link

	// Multi-Debrid Placement Strategy
	ActiveDebrid string                `msgpack:"active_debrid" json:"active_debrid"` // Current active debrid
	Placements   map[string]*Placement `msgpack:"placements" json:"placements"`       // debrid_name -> placement

	// Files (from debrid cache)
	Files map[string]*File `msgpack:"files" json:"files"` // filename -> File details

	// Debrid State (from active placement)
	State    string                    `msgpack:"state" json:"state"`       // This is for QBitTorrent compatibility
	Status   debridTypes.TorrentStatus `msgpack:"status" json:"status"`     // downloaded, downloading, queued, error
	Progress float64                   `msgpack:"progress" json:"progress"` // Download progress (0-100)
	Speed    int64                     `msgpack:"speed" json:"speed"`       // Download speed
	Seeders  int                       `msgpack:"seeders" json:"seeders"`   // Number of seeders

	IsComplete bool `msgpack:"is_complete" json:"is_complete"` // Ready for use
	Bad        bool `msgpack:"bad" json:"bad"`                 // Marked as bad/corrupted

	// Metadata
	Category    string   `msgpack:"category,omitempty" json:"category,omitempty"`         // Category (e.g., sonarr, radarr)
	Tags        []string `msgpack:"tags,omitempty" json:"tags,omitempty"`                 // User-defined tags
	MountPath   string   `msgpack:"mount_path" json:"mount_path"`                         // Mount path for this torrent
	SavePath    string   `msgpack:"save_path,omitempty" json:"save_path,omitempty"`       // Download/symlink folder
	ContentPath string   `msgpack:"content_path,omitempty" json:"content_path,omitempty"` // Final content path

	// Timestamps
	AddedOn     time.Time  `msgpack:"added_on" json:"added_on"`                             // When first added (from debrid)
	CreatedAt   time.Time  `msgpack:"created_at" json:"created_at"`                         // When created in manager
	UpdatedAt   time.Time  `msgpack:"updated_at" json:"updated_at"`                         // Last update time
	CompletedAt *time.Time `msgpack:"completed_at,omitempty" json:"completed_at,omitempty"` // When completed
	ImportedAt  *time.Time `msgpack:"imported_at,omitempty" json:"imported_at,omitempty"`   // When imported by Arr

	// Import Request Data (for processing)
	Action           string `msgpack:"action,omitempty" json:"action,omitempty"`                       // symlink, download, none
	DownloadUncached bool   `msgpack:"download_uncached,omitempty" json:"download_uncached,omitempty"` // Force uncached download
	CallbackURL      string `msgpack:"callback_url,omitempty" json:"callback_url,omitempty"`           // Callback URL for completion
	SkipMultiSeason  bool   `msgpack:"skip_multi_season,omitempty" json:"skip_multi_season,omitempty"` // Skip multi-season detection

	// Error tracking
	LastError     string     `msgpack:"last_error,omitempty" json:"last_error,omitempty"`           // Last error message
	ErrorCount    int        `msgpack:"error_count,omitempty" json:"error_count,omitempty"`         // Number of errors
	LastErrorTime *time.Time `msgpack:"last_error_time,omitempty" json:"last_error_time,omitempty"` // Last error time
}

type File struct {
	Name      string    `msgpack:"name" json:"name"`
	Size      int64     `msgpack:"size" json:"size"`
	IsRar     bool      `msgpack:"is_rar" json:"is_rar"`
	ByteRange *[2]int64 `msgpack:"byte_range,omitempty" json:"byte_range,omitempty"`
	Deleted   bool      `msgpack:"deleted" json:"deleted"`
}

// PlacementFile represents debrid-specific file information
type PlacementFile struct {
	Id   string `msgpack:"id,omitempty" json:"id,omitempty"`     // For TorBox-style providers (file_id)
	Link string `msgpack:"link,omitempty" json:"link,omitempty"` // For RealDebrid/AllDebrid-style providers (restricted URL)
	Path string `msgpack:"path,omitempty" json:"path,omitempty"` // Path within the debrid's filesystem
}

// Placement represents a torrent's placement on a specific debrid service
type Placement struct {
	Debrid    string                    `msgpack:"debrid,omitempty" json:"debrid,omitempty"`
	ID        string                    `msgpack:"debrid_id" json:"id"`                              // ID in that debrid service (e.g., L3734BKKKSBA6)
	AddedAt   time.Time                 `msgpack:"added_at" json:"added_at"`                         // When added to this debrid
	RemovedAt *time.Time                `msgpack:"removed_at,omitempty" json:"removed_at,omitempty"` // When removed (if archived)
	Status    debridTypes.TorrentStatus `msgpack:"status" json:"status"`                             // Placement status
	IsActive  bool                      `msgpack:"is_active" json:"is_active"`                       // Whether this is the active placement
	Progress  float64                   `msgpack:"progress" json:"progress"`                         // Download progress on this debrid (0-100)

	// Debrid-specific file information
	Files map[string]*PlacementFile `msgpack:"files" json:"files"` // filename -> debrid-specific file info

	// Cached data from debrid (avoid re-fetching)
	DownloadedAt *time.Time `msgpack:"downloaded_at,omitempty" json:"downloaded_at,omitempty"` // When download completed on debrid
}

// GetActivePlacement returns the active placement
func (t *Torrent) GetActivePlacement() *Placement {
	if t.ActiveDebrid == "" || t.Placements == nil {
		return nil
	}
	return t.Placements[t.ActiveDebrid]
}

// AddPlacement adds or updates a placement for a debrid
func (t *Torrent) AddPlacement(debridTorrent *debridTypes.Torrent) *Placement {
	if t.Placements == nil {
		t.Placements = make(map[string]*Placement)
	}

	placement := &Placement{
		Debrid:   debridTorrent.Debrid,
		ID:       debridTorrent.Id,
		AddedAt:  time.Now(),
		Status:   debridTorrent.Status,
		Files:    make(map[string]*PlacementFile),
		IsActive: false, // Will be set when activating
	}

	t.Placements[debridTorrent.Debrid] = placement
	return placement
}

// ActivatePlacement switches the active debrid
func (t *Torrent) ActivatePlacement(debridName string) error {
	if t.Placements == nil || t.Placements[debridName] == nil {
		return ErrPlacementNotFound
	}

	// Deactivate all placements
	for _, p := range t.Placements {
		p.IsActive = false
	}
	if t.Placements[debridName].Status != debridTypes.TorrentStatusCompleted {
		return ErrPlacementNotCompleted
	}

	// Activate the target placement
	t.Placements[debridName].IsActive = true
	t.ActiveDebrid = debridName
	t.UpdatedAt = time.Now()

	return nil
}

// RemovePlacement deletes a debrid torrent from the debrid itself
func (t *Torrent) RemovePlacement(debridName string, cleanup func(placement *Placement) error) {
	if t.Placements == nil || t.Placements[debridName] == nil {
		return
	}
	// Get the placement
	placement := t.Placements[debridName]

	// We want to remove the placement
	delete(t.Placements, debridName)

	// If the placement is the active placement, find a new active placement
	if t.ActiveDebrid == debridName {
		t.SwitchToNextPlacement()
	}

	// Call cleanup function if provided
	if cleanup != nil {
		_ = cleanup(placement)
	}
}

// HasPlacement checks if torrent exists on a debrid
func (t *Torrent) HasPlacement(debridName string) bool {
	if t.Placements == nil {
		return false
	}
	_, exists := t.Placements[debridName]
	return exists
}

// SwitchToNextPlacement switches to the next completed placement if available
func (t *Torrent) SwitchToNextPlacement() {
	if t.Placements == nil {
		return
	}
	for debridName, placement := range t.Placements {
		if placement.Status == debridTypes.TorrentStatusCompleted {
			_ = t.ActivatePlacement(debridName)
		}
	}
}

// IsCompleted checks if torrent is fully completed
func (t *Torrent) IsCompleted() bool {
	return t.Status == debridTypes.TorrentStatusCompleted && t.Progress >= 1.0 && t.ContentPath != ""
}

// MarkAsCompleted marks the torrent as completed
func (t *Torrent) MarkAsCompleted(contentPath string) {
	t.Status = debridTypes.TorrentStatusCompleted
	t.IsComplete = true
	t.Progress = 1.0
	t.ContentPath = contentPath
	now := time.Now()
	t.CompletedAt = &now
	t.UpdatedAt = now
}

func (t *Torrent) GetState() string {
	state := "downloading"
	switch t.Status {
	case debridTypes.TorrentStatusDownloading:
		state = "downloading"
	case debridTypes.TorrentStatusQueued:
		state = "pausedDL"
	case debridTypes.TorrentStatusError:
		state = "error"
	case debridTypes.TorrentStatusCompleted:
		state = "pausedUP"
	}
	return state
}

// MarkAsError marks the torrent as errored
func (t *Torrent) MarkAsError(err error) {
	t.Status = debridTypes.TorrentStatusError
	t.State = t.GetState()
	t.LastError = err.Error()
	t.ErrorCount++
	now := time.Now()
	t.LastErrorTime = &now
	t.UpdatedAt = now
}

func (t *Torrent) GetFile(filename string) (*File, error) {
	if t.Files == nil {
		return nil, fmt.Errorf("file not found")
	}
	f, exists := t.Files[filename]
	if !exists {
		return nil, fmt.Errorf("file not found")
	}
	if f.Deleted {
		return nil, fmt.Errorf("file deleted")
	}
	return f, nil
}

func (t *Torrent) GetActiveFiles() []*File {
	files := make([]*File, 0, len(t.Files))
	for _, f := range t.Files {
		if !f.Deleted {
			files = append(files, f)
		}
	}
	return files
}

// SwitcherJob tracks the progress of a migration operation
type SwitcherJob struct {
	ID           string         `msgpack:"id" json:"id"`
	InfoHash     string         `msgpack:"infohash" json:"info_hash"`                            // Torrent being migrated
	SourceDebrid string         `msgpack:"source_debrid" json:"source_debrid"`                   // Source debrid
	TargetDebrid string         `msgpack:"target_debrid" json:"target_debrid"`                   // Target debrid
	Status       SwitcherStatus `msgpack:"status" json:"status"`                                 // Job status
	Progress     float64        `msgpack:"progress" json:"progress"`                             // Progress (0-100)
	Error        string         `msgpack:"error,omitempty" json:"error,omitempty"`               // Error message if failed
	CreatedAt    time.Time      `msgpack:"created_at" json:"created_at"`                         // When job started
	CompletedAt  *time.Time     `msgpack:"completed_at,omitempty" json:"completed_at,omitempty"` // When completed
	KeepOld      bool           `msgpack:"keep_old" json:"keep_old"`                             // Whether to keep old placement(or remove it)
	WaitComplete bool           `msgpack:"wait_complete" json:"wait_complete"`                   // Whether to wait for download
}

// SystemMigrationStatus tracks overall system migration from legacy to unified
type SystemMigrationStatus struct {
	Running   bool      `msgpack:"running" json:"running"`                           // Whether migration is running
	Total     int       `msgpack:"total" json:"total"`                               // Total torrents to migrate
	Completed int       `msgpack:"completed" json:"completed"`                       // Completed migrations
	Errors    int       `msgpack:"errors" json:"errors"`                             // Number of errors
	StartedAt time.Time `msgpack:"started_at" json:"started_at"`                     // When migration started
	UpdatedAt time.Time `msgpack:"updated_at" json:"updated_at"`                     // Last update
	ErrorList []string  `msgpack:"error_list,omitempty" json:"error_list,omitempty"` // List of errors
}

// CachedTorrent represents the debrid cache JSON format for migration
type CachedTorrent struct {
	ID               string                       `json:"id"`                // Debrid torrent ID
	InfoHash         string                       `json:"info_hash"`         // Torrent info hash
	Name             string                       `json:"name"`              // Torrent name
	Folder           string                       `json:"folder"`            // Folder name
	Filename         string                       `json:"filename"`          // Filename
	OriginalFilename string                       `json:"original_filename"` // Original filename
	Size             int64                        `json:"size"`              // Size (legacy)
	Bytes            int64                        `json:"bytes"`             // Actual bytes
	Magnet           interface{}                  `json:"magnet"`            // Magnet (can be nil)
	Files            map[string]*debridTypes.File `json:"files"`             // Files map
	Status           string                       `json:"status"`            // Status from debrid
	Added            string                       `json:"added"`             // Added timestamp
	Progress         float64                      `json:"progress"`          // Progress 0-100
	Speed            int64                        `json:"speed"`             // Speed
	Seeders          int                          `json:"seeders"`           // Seeders
	Links            []string                     `json:"links"`             // Download links
	MountPath        string                       `json:"mount_path"`        // Mount path
	DeletedFiles     []string                     `json:"deleted_files"`     // Deleted files
	Debrid           string                       `json:"debrid"`            // Debrid name
	Arr              *arr.Arr                     `json:"arr"`               // Arr association
	AddedOn          string                       `json:"added_on"`          // Added on timestamp
	IsComplete       bool                         `json:"is_complete"`       // Is complete
	Bad              bool                         `json:"bad"`               // Is bad
}

// ToManagedTorrent converts a cached torrent to managed format
func (ct *CachedTorrent) ToManagedTorrent() *Torrent {
	now := time.Now()

	// Parse timestamps
	var addedOn, createdAt time.Time
	if ct.AddedOn != "" {
		addedOn, _ = time.Parse(time.RFC3339, ct.AddedOn)
	}
	if addedOn.IsZero() && ct.Added != "" {
		addedOn, _ = time.Parse(time.RFC3339, ct.Added)
	}
	if addedOn.IsZero() {
		addedOn = now
	}
	createdAt = addedOn
	// Get category from arr
	var category string
	if ct.Arr != nil {
		category = ct.Arr.Name
	}

	mt := &Torrent{
		InfoHash:         ct.InfoHash,
		Name:             ct.Name,
		Folder:           ct.Folder,
		OriginalFilename: ct.OriginalFilename,
		Size:             ct.Size,
		Bytes:            ct.Bytes,
		Magnet:           "",
		ActiveDebrid:     ct.Debrid,
		Placements:       make(map[string]*Placement),
		Status:           debridTypes.TorrentStatus(ct.Status),
		Progress:         ct.Progress,
		Speed:            ct.Speed,
		Seeders:          ct.Seeders,
		IsComplete:       ct.IsComplete,
		Bad:              ct.Bad,
		Category:         category,
		Tags:             []string{},
		MountPath:        ct.MountPath,
		AddedOn:          addedOn,
		CreatedAt:        createdAt,
		UpdatedAt:        now,
		Files:            make(map[string]*File),
	}

	mt.Folder = GetTorrentFolder(mt)

	for fname, f := range ct.Files {
		mt.Files[fname] = &File{
			Name:      f.Name,
			Size:      f.Size,
			IsRar:     f.IsRar,
			ByteRange: f.ByteRange,
		}
	}

	// Set magnet if present
	if ct.Magnet != nil {
		if mag, ok := ct.Magnet.(string); ok {
			mt.Magnet = mag
		}
	}

	// Create placement for this debrid
	if ct.Debrid != "" && ct.ID != "" {
		var downloadedAt *time.Time
		if ct.IsComplete {
			downloadedAt = &addedOn
		}

		placement := &Placement{
			Debrid:       ct.Debrid,
			ID:           ct.ID,
			AddedAt:      addedOn,
			Status:       debridTypes.TorrentStatus(ct.Status),
			IsActive:     true,
			Progress:     ct.Progress,
			DownloadedAt: downloadedAt,
			Files:        make(map[string]*PlacementFile),
		}

		// Populate placement files from cached torrent
		for _, f := range ct.Files {
			placement.Files[f.Name] = &PlacementFile{
				Id:   f.Id,
				Link: f.Link,
				Path: f.Path,
			}
		}

		mt.Placements[ct.Debrid] = placement
	}

	// Set completion timestamp if complete
	if ct.IsComplete {
		mt.CompletedAt = &addedOn
	}

	return mt
}

// GetTorrentFolder returns the folder name for a torrent by debrid ID
func GetTorrentFolder(torrent *Torrent) string {
	cfg := config.Get()
	switch cfg.Manager.FolderNaming {
	case config.WebDavUseFileName:
		return path.Clean(torrent.Name)
	case config.WebDavUseOriginalName:
		return path.Clean(torrent.OriginalFilename)
	case config.WebDavUseFileNameNoExt:
		return path.Clean(utils.RemoveExtension(torrent.Name))
	case config.WebDavUseOriginalNameNoExt:
		return path.Clean(utils.RemoveExtension(torrent.OriginalFilename))
	case config.WebdavUseHash:
		return strings.ToLower(torrent.InfoHash)
	default:
		return path.Clean(torrent.Name)
	}
}
