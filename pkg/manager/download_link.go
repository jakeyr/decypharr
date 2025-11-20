package manager

import (
	"fmt"

	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// GetDownloadLink gets or fetches a download link for a file
func (m *Manager) GetDownloadLink(torrent *storage.Torrent, filename string) (types.DownloadLink, error) {
	// GetReader the file
	file, err := torrent.GetFile(filename)
	if err != nil {
		return types.DownloadLink{}, fmt.Errorf("file %s not found in torrent %s: %w", filename, torrent.Name, err)
	}
	// GetReader placement and placement file
	placementFile, err := m.getPlacementFile(torrent, filename)
	if err != nil {
		return types.DownloadLink{}, err
	}

	// Use the file link/id as the cache key
	fileLink := placementFile.Link
	if fileLink == "" {
		return types.DownloadLink{}, fmt.Errorf("file link is missing for %s in torrent %s after refresh", filename, torrent.Name)
	}

	// Use singleflight to deduplicate concurrent requests
	v, err, _ := m.downloadSG.Do(fileLink, func() (interface{}, error) {
		// Double-check cache inside singleflight
		// Fetch the download link
		dl, err := m.fetchDownloadLink(torrent, file, placementFile, torrent.ActiveDebrid)
		if err != nil {
			m.downloadSG.Forget(fileLink)
			return types.DownloadLink{}, err
		}

		if dl.Empty() {
			m.downloadSG.Forget(fileLink)
			return types.DownloadLink{}, fmt.Errorf("download link is empty for %s in torrent %s", filename, torrent.Name)
		}

		return dl, nil
	})

	if err != nil {
		return types.DownloadLink{}, err
	}
	return v.(types.DownloadLink), nil
}

// getPlacementFile retrieves the placement file with refresh/repair fallback
func (m *Manager) getPlacementFile(torrent *storage.Torrent, filename string) (*storage.PlacementFile, error) {
	// GetReader the file to determine which infohash and debrid it belongs to
	_, ok := torrent.Files[filename]
	if !ok {
		return nil, fmt.Errorf("file %s not found in torrent", filename)
	}
	placement := torrent.Placements[torrent.ActiveDebrid]
	if placement == nil {
		return nil, fmt.Errorf("no placement found for debrid %s with infohash %s", torrent.ActiveDebrid, torrent.InfoHash)
	}

	// GetReader placement-specific file info
	placementFile := placement.Files[filename]
	if placementFile == nil || (placementFile.Link == "" && placementFile.Id == "") {
		// File not in placement or missing link, try refreshing
		refreshed, err := m.refreshTorrent(torrent.InfoHash)
		if err != nil {
			return nil, fmt.Errorf("failed to refresh torrent: %w", err)
		}

		// Re-fetch file and placement after refresh
		file := refreshed.Files[filename]
		if file == nil {
			return nil, fmt.Errorf("file disappeared after refresh")
		}
		placement = refreshed.Placements[torrent.ActiveDebrid]
		if placement == nil {
			return nil, fmt.Errorf("placement disappeared after refresh for debrid %s", torrent.ActiveDebrid)
		}

		placementFile = placement.Files[filename]

		// Still missing after refresh?
		if placementFile == nil || (placementFile.Link == "" && placementFile.Id == "") {
			return nil, fmt.Errorf("file %s not available after refresh", filename)
		}
	}

	return placementFile, nil
}

// fetchDownloadLink fetches a download link from the debrid service
func (m *Manager) fetchDownloadLink(torrent *storage.Torrent, file *storage.File, placementFile *storage.PlacementFile, debridName string) (types.DownloadLink, error) {
	emptyDownloadLink := types.DownloadLink{}

	client := m.DebridClient(debridName)
	if client == nil {
		return emptyDownloadLink, fmt.Errorf("debrid client not found: %s", debridName)
	}

	placement := torrent.Placements[torrent.ActiveDebrid]
	if placement == nil {
		return emptyDownloadLink, fmt.Errorf("no placement found for debrid %s with infohash %s", debridName, torrent.InfoHash)
	}

	// Convert to types.File for client call
	debridFile := types.File{
		Id:        placementFile.Id,
		Link:      placementFile.Link,
		Path:      placementFile.Path,
		Name:      file.Name,
		Size:      file.Size,
		IsRar:     file.IsRar,
		ByteRange: file.ByteRange,
		Deleted:   file.Deleted,
	}
	downloadLink, err := client.GetDownloadLink(placement.ID, &debridFile)
	if err != nil {
		return types.DownloadLink{}, err
	}
	return downloadLink, nil
}

// GetDownloadByteRange gets the byte range for a file
func (m *Manager) GetDownloadByteRange(torrentName, filename string) (*[2]int64, error) {
	entry, err := m.storage.GetEntry(torrentName)
	if err != nil {
		return nil, fmt.Errorf("torrent not found: %w", err)
	}

	file, ok := entry.Files[filename]
	if !ok {
		return nil, fmt.Errorf("file %s not found in torrent", filename)
	}

	return file.ByteRange, nil
}

// GetTotalActiveDownloadLinks returns the total number of active download links across all debrids
func (m *Manager) GetTotalActiveDownloadLinks() int {
	total := 0

	m.clients.Range(func(name string, client debrid.Client) bool {
		if client == nil {
			return true
		}

		allAccounts := client.AccountManager().Active()
		for _, acc := range allAccounts {
			total += acc.DownloadLinksCount()
		}

		return true
	})

	return total
}
