package manager

import (
	"context"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirrobot01/decypharr/internal/utils"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	torrentpkg "github.com/sirrobot01/decypharr/pkg/storage"
)

// refreshTorrents refreshes torrents from a specific debrid service
func (m *Manager) refreshTorrents(ctx context.Context, debridName string, debridClient debrid.Client) {
	select {
	case <-ctx.Done():
		return
	default:
	}
	defer func() {
		runtime.GC()
		debug.FreeOSMemory()
	}()

	// Use singleflight to prevent concurrent refreshes for the same debrid
	_, err, _ := m.refreshSG.Do(debridName, func() (interface{}, error) {
		m.logger.Debug().Str("debrid", debridName).Msg("Starting torrent refresh")

		// Fetch torrents from this debrid client
		torrents, err := debridClient.GetTorrents()
		if err != nil {
			m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to get torrents")
			return nil, err
		}

		if len(torrents) == 0 {
			m.logger.Debug().Str("debrid", debridName).Msg("No torrents found")
			return nil, nil
		}

		// Get all cached torrents
		cachedTorrents, err := m.storage.List(nil)
		if err != nil {
			m.logger.Error().Err(err).Msg("Failed to list cached torrents")
			return nil, err
		}

		// Build map of cached torrents by infohash
		cachedMap := make(map[string]*torrentpkg.Torrent)
		for _, t := range cachedTorrents {
			cachedMap[t.InfoHash] = t
		}

		// Build map of current debrid torrents by ID
		currentTorrentIds := make(map[string]string) // debrid_id -> infohash
		for _, t := range torrents {
			currentTorrentIds[t.Id] = t.InfoHash
		}

		// Check for deleted placements on this debrid
		deletedPlacements := make([]struct{ infohash, debridName string }, 0)
		for _, cached := range cachedMap {
			placement, hasPlacement := cached.Placements[debridName]
			if hasPlacement {
				// Check if this placement still exists on the debrid
				if _, exists := currentTorrentIds[placement.ID]; !exists {
					deletedPlacements = append(deletedPlacements, struct{ infohash, debridName string }{cached.InfoHash, debridName})
				}
			}
		}

		// Handle deleted placements
		if len(deletedPlacements) > 0 {
			m.logger.Info().Str("debrid", debridName).Msgf("Found %d deleted placements", len(deletedPlacements))
			for _, dp := range deletedPlacements {
				if torrent, ok := cachedMap[dp.infohash]; ok {
					torrent.RemovePlacement(dp.debridName, nil)

					// If no placements left, delete the entire torrent
					if len(torrent.Placements) == 0 {
						if err := m.storage.Delete(dp.infohash); err != nil {
							m.logger.Error().Err(err).Str("infohash", dp.infohash).Msg("Failed to delete torrent")
						}
						delete(cachedMap, dp.infohash)
					} else {
						// Update the torrent with removed placement
						if err := m.storage.Update(torrent); err != nil {
							m.logger.Error().Err(err).Str("infohash", dp.infohash).Msg("Failed to update torrent")
						}
					}
				}
			}
		}

		// Find new torrents or new placements
		newTorrents := make([]*types.Torrent, 0)
		for _, t := range torrents {
			cached, exists := cachedMap[t.InfoHash]

			if !exists {
				// Brand new torrent
				newTorrents = append(newTorrents, t)
			} else if !cached.HasPlacement(debridName) {
				// Existing torrent but new placement on this debrid
				newTorrents = append(newTorrents, t)
			} else {
				// Update existing placement progress/status
				placement := cached.Placements[debridName]
				if placement.ID == t.Id {
					placement.Status = t.Status
					placement.Progress = t.Progress
					_ = m.storage.Update(cached)
				}
			}
		}

		// Process new torrents/placements
		if len(newTorrents) > 0 {
			m.logger.Trace().Str("debrid", debridName).Msgf("Found %d new torrents/placements", len(newTorrents))

			workChan := make(chan *types.Torrent, min(100, len(newTorrents)))
			var processWg sync.WaitGroup
			var processed int64

			workers := 10
			for i := 0; i < workers; i++ {
				processWg.Add(1)
				go func() {
					defer processWg.Done()
					for t := range workChan {
						if err := m.processSyncTorrent(t, cachedMap); err != nil {
							m.logger.Error().Err(err).Str("debrid", debridName).Msgf("Failed to process torrent %s", t.Id)
						}
						atomic.AddInt64(&processed, 1)
					}
				}()
			}

			for _, t := range newTorrents {
				workChan <- t
			}
			close(workChan)
			processWg.Wait()

			m.logger.Debug().Str("debrid", debridName).Msgf("Processed %d new torrents/placements", processed)
		}

		return nil, nil
	})

	if err != nil {
		m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to refresh torrents")
	}
}

// processSyncTorrent processes a single torrent during refresh
func (m *Manager) processSyncTorrent(t *types.Torrent, cachedMap map[string]*torrentpkg.Torrent) error {
	// Get the debrid client
	client := m.DebridClient(t.Debrid)
	if client == nil {
		return nil
	}

	// Check if files are complete
	if len(t.Files) == 0 || !isComplete(t.Files) {
		if err := client.UpdateTorrent(t); err != nil {
			return err
		}
	}

	if !isComplete(t.Files) {
		m.logger.Debug().
			Str("torrent_id", t.Id).
			Str("torrent_name", t.Name).
			Msg("Torrent still not complete after refresh")
		return nil
	}

	// Parse added timestamp
	addedOn, err := time.Parse(time.RFC3339, t.Added)
	if err != nil {
		addedOn = time.Now()
	}

	// Check if we have an existing managed torrent
	mt, exists := cachedMap[t.InfoHash]
	if !exists {
		// Create new managed torrent
		var magnet *utils.Magnet
		if t.Magnet == nil || t.Magnet.Link == "" {
			magnet = utils.ConstructMagnet(t.InfoHash, t.Name)
			if err != nil {
				return err
			}
		} else {
			magnet = t.Magnet
		}
		size := t.Size
		if size == 0 {
			size = t.Bytes
		}
		mt = &torrentpkg.Torrent{
			InfoHash:         t.InfoHash,
			Name:             t.Name,
			OriginalFilename: t.OriginalFilename,
			Size:             size,
			Bytes:            size,
			Magnet:           magnet.Link,
			ActiveDebrid:     t.Debrid,
			Placements:       make(map[string]*torrentpkg.Placement),
			Files:            make(map[string]*torrentpkg.File),
			Status:           t.Status,
			Progress:         t.Progress,
			Speed:            t.Speed,
			Seeders:          t.Seeders,
			IsComplete:       len(t.Files) > 0,
			Bad:              false,
			AddedOn:          addedOn,
			CreatedAt:        addedOn,
			UpdatedAt:        time.Now(),
		}
		mt.Folder = torrentpkg.GetTorrentFolder(mt)
		cachedMap[t.InfoHash] = mt
	}

	// Populate global Files metadata (only if empty)
	if len(mt.Files) == 0 {
		for _, f := range t.GetFiles() {
			mt.Files[f.Name] = &torrentpkg.File{
				Name:      f.Name,
				Size:      f.Size,
				IsRar:     f.IsRar,
				ByteRange: f.ByteRange,
				Deleted:   f.Deleted,
			}
		}
	}

	// Add or update placement
	placement := mt.AddPlacement(t)
	placement.Progress = t.Progress
	if t.Status == types.TorrentStatusCompleted {
		downloadedAt := addedOn
		placement.DownloadedAt = &downloadedAt
	}

	// Populate placement-specific file data
	for _, f := range t.GetFiles() {
		placement.Files[f.Name] = &torrentpkg.PlacementFile{
			Id:   f.Id,
			Link: f.Link,
			Path: f.Path,
		}
	}

	// If this is the first placement or the only one, make it active
	if mt.ActiveDebrid == "" || len(mt.Placements) == 1 {
		if t.Status == types.TorrentStatusCompleted {
			_ = mt.ActivatePlacement(t.Debrid)
		}
	}

	// Save to storage
	return m.storage.Add(mt)
}

// refreshTorrent refreshes a single torrent from its active debrid
func (m *Manager) refreshTorrent(infohash string) (*torrentpkg.Torrent, error) {
	torrent, err := m.storage.Get(infohash)
	if err != nil {
		return nil, err
	}

	if torrent.ActiveDebrid == "" {
		return torrent, nil
	}

	client := m.DebridClient(torrent.ActiveDebrid)
	if client == nil {
		return torrent, nil
	}

	placement := torrent.GetActivePlacement()
	if placement == nil {
		return torrent, nil
	}

	// Get updated torrent info from debrid
	debridTorrent, err := client.GetTorrent(placement.ID)
	if err != nil {
		return nil, err
	}

	// Update placement info
	placement.Status = debridTorrent.Status
	placement.Progress = debridTorrent.Progress

	// Update global Files metadata (only if needed)
	for _, f := range debridTorrent.GetFiles() {
		if _, exists := torrent.Files[f.Name]; !exists {
			torrent.Files[f.Name] = &torrentpkg.File{
				Name:      f.Name,
				Size:      f.Size,
				IsRar:     f.IsRar,
				ByteRange: f.ByteRange,
				Deleted:   f.Deleted,
			}
		}
	}

	// Update placement-specific file data
	for _, f := range debridTorrent.GetFiles() {
		placement.Files[f.Name] = &torrentpkg.PlacementFile{
			Id:   f.Id,
			Link: f.Link,
			Path: f.Path,
		}
	}

	torrent.Status = debridTorrent.Status
	torrent.Progress = debridTorrent.Progress
	torrent.UpdatedAt = time.Now()

	// Save to storage
	if err := m.storage.Update(torrent); err != nil {
		return nil, err
	}

	return torrent, nil
}

// refreshDebridDownloadLinks refreshes download links for a specific debrid service
func (m *Manager) refreshDownloadLinks(ctx context.Context, debridName string, debridClient debrid.Client) {
	defer func() {
		runtime.GC()
		debug.FreeOSMemory()
	}()

	select {
	case <-ctx.Done():
		return
	default:
	}

	if debridClient == nil {
		m.logger.Warn().Str("debrid", debridName).Msg("Debrid client is nil, skipping download link refresh")
		return
	}

	if err := debridClient.RefreshDownloadLinks(); err != nil {
		m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to refresh download links")
	} else {
		m.logger.Debug().Str("debrid", debridName).Msg("Refreshed download links")
	}
}

// isComplete checks if all files in a torrent have download links
func isComplete(files map[string]types.File) bool {
	if len(files) == 0 {
		return false
	}
	for _, file := range files {
		if file.Link == "" {
			return false
		}
	}
	return true
}
