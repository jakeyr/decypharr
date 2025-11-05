package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	torrentpkg "github.com/sirrobot01/decypharr/pkg/storage"
)

// AddTorrent creates a torrent from import request and processes it
func (m *Manager) AddTorrent(ctx context.Context, importReq *ImportRequest) error {
	// Create managed torrent with InfoHash as primary key
	torrent := &torrentpkg.Torrent{
		InfoHash:         importReq.Magnet.InfoHash,
		Name:             importReq.Magnet.Name,
		Size:             importReq.Magnet.Size,
		Bytes:            importReq.Magnet.Size,
		Magnet:           importReq.Magnet.Link,
		Category:         importReq.Arr.Name,
		SavePath:         importReq.DownloadFolder,
		Status:           debridTypes.TorrentStatusPending,
		Progress:         0,
		Action:           importReq.Action,
		DownloadUncached: importReq.DownloadUncached,
		CallbackURL:      importReq.CallBackUrl,
		SkipMultiSeason:  importReq.SkipMultiSeason,
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
		AddedOn:          time.Now(),
		Placements:       make(map[string]*torrentpkg.Placement),
		Files:            make(map[string]*torrentpkg.File),
		Tags:             []string{},
	}

	torrent.Folder = torrentpkg.GetTorrentFolder(torrent)

	// Save to storage
	if err := m.queue.Add(torrent); err != nil {
		return fmt.Errorf("failed to save torrent: %w", err)
	}

	// Process in background
	go m.processNewTorrent(ctx, torrent, importReq)

	return nil
}

// processTorrent handles the complete torrent lifecycle
// This replaces wire.Store.processFiles completely
func (m *Manager) processNewTorrent(ctx context.Context, torrent *torrentpkg.Torrent, importReq *ImportRequest) {
	// Update status to submitting
	torrent.Status = debridTypes.TorrentStatusProcessing
	torrent.UpdatedAt = time.Now()
	_ = m.queue.Update(torrent)

	// Submit to debrid using integrated method
	debridTorrent, err := m.SendToDebrid(ctx, importReq)
	if err != nil {
		// Check if too many active downloads
		var httpErr *utils.HTTPError
		if errors.As(err, &httpErr) && httpErr.Code == "too_many_active_downloads" {
			m.logger.Warn().Msgf("Too many active downloads, marking as queued: %s", torrent.Name)
			torrent.Status = debridTypes.TorrentStatusQueued
			torrent.UpdatedAt = time.Now()
			if err := m.queue.ReQueue(importReq); err != nil {
				m.logger.Error().Err(err).Msg("Failed to re-queue torrent")
				return
			}
			_ = m.queue.Update(torrent)
			return
		}

		m.logger.Error().Err(err).Str("name", torrent.Name).Msg("Failed to submit torrent to debrid")
		torrent.MarkAsError(err)
		_ = m.queue.Update(torrent)
		return
	}

	// Add placement
	placement := torrent.AddPlacement(debridTorrent)
	placement.IsActive = true
	torrent.ActiveDebrid = debridTorrent.Debrid
	torrent.Status = debridTypes.TorrentStatusDownloading
	torrent.UpdatedAt = time.Now()
	// Add files here
	for _, file := range debridTorrent.Files {
		tFile := &torrentpkg.File{
			Name:      file.Name,
			Size:      file.Size,
			ByteRange: file.ByteRange,
			Deleted:   file.Deleted,
			IsRar:     file.IsRar,
		}
		torrent.Files[file.Name] = tFile
	}
	_ = m.queue.Update(torrent)

	// Get debrid client
	client := m.DebridClient(debridTorrent.Debrid)
	if client == nil {
		m.logger.Error().Str("debrid", debridTorrent.Debrid).Msg("Debrid client not found")
		torrent.MarkAsError(fmt.Errorf("debrid client not found: %s", debridTorrent.Debrid))
		_ = m.queue.Update(torrent)
		return
	}
	downloadingStatuses := client.GetDownloadingStatus()

	// Monitor download progress
	backoff := time.NewTimer(m.refreshInterval)
	defer backoff.Stop()

	for debridTorrent.Status != "downloaded" {
		select {
		case <-ctx.Done():
			m.logger.Info().Msg("Context cancelled, stopping torrent processing")
			return
		case <-backoff.C:
		}

		// Check status
		dbT, err := client.CheckStatus(debridTorrent)
		if err != nil {
			m.logger.Error().Err(err).Str("name", torrent.Name).Msg("Error checking status")
			torrent.MarkAsError(err)
			_ = m.queue.Update(torrent)

			// Delete from debrid on error
			go func() {
				if dbT != nil && dbT.Id != "" {
					_ = client.DeleteTorrent(dbT.Id)
				}
			}()
			return
		}

		debridTorrent = dbT

		// Update torrent progress
		torrent.Progress = debridTorrent.Progress / 100.0
		torrent.Speed = debridTorrent.Speed
		torrent.Seeders = debridTorrent.Seeders
		torrent.UpdatedAt = time.Now()

		// Update placement progress
		if placement := torrent.GetActivePlacement(); placement != nil {
			placement.Progress = torrent.Progress
		}

		_ = m.queue.Update(torrent)

		m.logger.Debug().
			Str("debrid", debridTorrent.Debrid).
			Str("name", debridTorrent.Name).
			Float64("progress", debridTorrent.Progress).
			Msg("Download progress")

		// Check if done or failed
		if debridTorrent.Status == "downloaded" || !utils.Contains(downloadingStatuses, string(debridTorrent.Status)) {
			break
		}

		// Reset backoff
		nextInterval := min(m.refreshInterval*2, 30*time.Second)
		backoff.Reset(nextInterval)
	}

	// Mark placement as downloaded
	if placement := torrent.GetActivePlacement(); placement != nil {
		now := time.Now()
		placement.DownloadedAt = &now
		placement.Progress = 1.0
	}

	// Process post-download action
	torrent.Status = debridTypes.TorrentStatusDownloading
	torrent.UpdatedAt = time.Now()
	_ = m.queue.Update(torrent)
	switch torrent.Action {
	case "symlink":
		m.processSymlinkAction(torrent, debridTorrent)
	case "download":
		// get all files links
		if err := client.GetFileDownloadLinks(debridTorrent); err != nil {
			m.logger.Error().Err(err).Str("name", torrent.Name).Msg("Failed to get file download links")
			m.markDownloadAsError(debridTorrent, torrent, err)
			return
		}

		m.processDownloadAction(torrent, debridTorrent)
	default:
		m.markDownloadAsComplete(debridTorrent, torrent)
	}
}

// SendToDebrid submits a magnet to debrid service(s) - replaces debrid.Process
func (m *Manager) SendToDebrid(ctx context.Context, importRequest *ImportRequest) (*debridTypes.Torrent, error) {
	debridTorrent := &debridTypes.Torrent{
		InfoHash: importRequest.Magnet.InfoHash,
		Magnet:   importRequest.Magnet,
		Name:     importRequest.Magnet.Name,
		Arr:      importRequest.Arr,
		Size:     importRequest.Magnet.Size,
		Files:    make(map[string]debridTypes.File),
	}

	clients := m.FilterDebrid(func(c common.Client) bool {
		if importRequest.SelectedDebrid != "" && c.Config().Name != importRequest.SelectedDebrid {
			return false
		}
		return true
	})

	if len(clients) == 0 {
		return nil, fmt.Errorf("no debrid clients available")
	}

	errs := make([]error, 0, len(clients))

	overrideDownloadUncached := importRequest.DownloadUncached
	// Override first, arr second, debrid third
	if !overrideDownloadUncached && importRequest.Arr.DownloadUncached != nil {
		// Arr cached is set
		overrideDownloadUncached = *importRequest.Arr.DownloadUncached
	}

	for _, db := range clients {
		_logger := db.Logger()
		_logger.Info().
			Str("Debrid", db.Config().Name).
			Str("Arr", importRequest.Arr.Name).
			Str("Hash", debridTorrent.InfoHash).
			Str("Name", debridTorrent.Name).
			Str("Action", importRequest.Action).
			Msg("Processing torrent")

		// If debrid.DownloadUncached is true, it overrides everything
		if db.Config().DownloadUncached || overrideDownloadUncached {
			debridTorrent.DownloadUncached = true
		}

		dbt, err := db.SubmitMagnet(debridTorrent)
		if err != nil || dbt == nil || dbt.Id == "" {
			errs = append(errs, err)
			continue
		}
		dbt.Arr = importRequest.Arr
		_logger.Info().Str("id", dbt.Id).Msgf("Torrent: %s submitted to %s", dbt.Name, db.Config().Name)

		torrent, err := db.CheckStatus(dbt)
		if err != nil && torrent != nil && torrent.Id != "" {
			// Delete the torrent if it was not downloaded
			go func(id string) {
				_ = db.DeleteTorrent(id)
			}(torrent.Id)
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if torrent == nil {
			errs = append(errs, fmt.Errorf("torrent %s returned nil after checking status", dbt.Name))
			continue
		}
		return torrent, nil
	}
	if len(errs) == 0 {
		return nil, fmt.Errorf("failed to process torrent: no clients available")
	}
	joinedErrors := errors.Join(errs...)
	return nil, fmt.Errorf("failed to process torrent: %w", joinedErrors)
}

// sendCallback sends a callback HTTP request with torrent status
func (m *Manager) sendCallback(callbackURL string, torrent *torrentpkg.Torrent, status string, err error) {
	if callbackURL == "" {
		return
	}

	// Create payload
	payload := map[string]interface{}{
		"hash":     torrent.InfoHash,
		"name":     torrent.Name,
		"status":   status,
		"category": torrent.Category,
		"debrid":   torrent.ActiveDebrid,
	}

	if err != nil {
		payload["error"] = err.Error()
	}

	if torrent.ContentPath != "" {
		payload["content_path"] = torrent.ContentPath
	}

	data, jsonErr := json.Marshal(payload)
	if jsonErr != nil {
		m.logger.Error().Err(jsonErr).Msg("Failed to marshal callback payload")
		return
	}

	client := request.New()
	req, reqErr := http.NewRequest("POST", callbackURL, bytes.NewReader(data))
	if reqErr != nil {
		m.logger.Error().Err(reqErr).Msg("Failed to create callback request")
		return
	}

	req.Header.Set("Content-Type", "application/json")
	_, _ = client.Do(req)
}
