package manager

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/sirrobot01/decypharr/internal/utils"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// GetDownloadLink gets or fetches a download link for a file
func (m *Manager) GetDownloadLink(torrent *storage.Torrent, filename string) (types.DownloadLink, error) {

	// Get the file
	file, ok := torrent.Files[filename]
	if !ok {
		return types.DownloadLink{}, fmt.Errorf("file %s not found in torrent %s", filename, torrent.Folder)
	}

	// Use active debrid if not specified
	debridName := torrent.ActiveDebrid

	if debridName == "" {
		return types.DownloadLink{}, fmt.Errorf("no active debrid for torrent %s", torrent.Folder)
	}

	// Get placement and placement file
	placementFile, err := m.getPlacementFile(torrent, debridName, filename)
	if err != nil {
		return types.DownloadLink{}, err
	}

	// Use the file link/id as the cache key
	fileLink := placementFile.Link
	if fileLink == "" {
		fileLink = placementFile.Id
	}
	if fileLink == "" {
		return types.DownloadLink{}, fmt.Errorf("file link/id is empty for %s in torrent %s", filename, torrent.Folder)
	}

	// Check failure counter
	counter, ok := m.failedLinksCounter.Load(fileLink)
	if ok && counter.Load() >= int32(m.config.Retries) {
		return types.DownloadLink{}, fmt.Errorf("file link generation %s has failed %d times, not retrying", fileLink, counter.Load())
	}

	// Use singleflight to deduplicate concurrent requests
	v, err, _ := m.downloadSG.Do(fileLink, func() (interface{}, error) {
		// Double-check cache inside singleflight
		if dl, err := m.checkDownloadLink(fileLink, debridName); err == nil && !dl.Empty() {
			return dl, nil
		}

		// Fetch the download link
		dl, err := m.fetchDownloadLink(torrent, file, placementFile, debridName)
		if err != nil {
			m.downloadSG.Forget(fileLink)
			return types.DownloadLink{}, err
		}

		if dl.Empty() {
			m.downloadSG.Forget(fileLink)
			return types.DownloadLink{}, fmt.Errorf("download link is empty for %s in torrent %s", filename, torrent)
		}

		return dl, nil
	})

	if err != nil {
		return types.DownloadLink{}, err
	}
	return v.(types.DownloadLink), nil
}

// getPlacementFile retrieves the placement file with refresh/repair fallback
func (m *Manager) getPlacementFile(torrent *storage.Torrent, debridName, filename string) (*storage.PlacementFile, error) {
	placement := torrent.Placements[debridName]
	if placement == nil {
		return nil, fmt.Errorf("no placement found for debrid %s", debridName)
	}

	// Get placement-specific file info
	placementFile := placement.Files[filename]
	if placementFile == nil || (placementFile.Link == "" && placementFile.Id == "") {
		// File not in placement or missing link, try refreshing
		refreshed, err := m.refreshTorrent(torrent.InfoHash)
		if err != nil {
			return nil, fmt.Errorf("failed to refresh torrent: %w", err)
		}
		torrent = refreshed
		placement = torrent.Placements[debridName]
		if placement == nil {
			return nil, fmt.Errorf("placement disappeared after refresh for debrid %s", debridName)
		}
		placementFile = placement.Files[filename]

		// Still missing after refresh? Try repair
		if placementFile == nil || (placementFile.Link == "" && placementFile.Id == "") {
			return nil, fmt.Errorf("file %s not available after refresh and repair", filename)
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

	placement := torrent.Placements[debridName]
	if placement == nil {
		return emptyDownloadLink, fmt.Errorf("no placement found for debrid %s", debridName)
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

	// Build debrid torrent for GetDownloadLink call
	debridTorrent := &types.Torrent{
		Id:       placement.ID,
		InfoHash: torrent.InfoHash,
		Name:     torrent.Name,
		Debrid:   debridName,
	}

	m.logger.Trace().Msgf("Getting download link for %s(%s)", file.Name, placementFile.Link)
	downloadLink, err := client.GetDownloadLink(debridTorrent, &debridFile)
	if err != nil {
		if errors.Is(err, utils.HosterUnavailableError) {
			// Hoster unavailable - trigger repair
			m.logger.Debug().
				Str("filename", file.Name).
				Str("debrid", debridName).
				Msg("Hoster unavailable, attempting repair")

			success, moveErr := m.fixer.ReInsertTorrent(torrent)
			if moveErr != nil || !success {
				return emptyDownloadLink, fmt.Errorf("failed to repair torrent after hoster unavailable: %w", moveErr)
			}

			// Retry with potentially new debrid
			newTorrent, err := m.GetTorrent(torrent.InfoHash)
			if err != nil {
				return emptyDownloadLink, fmt.Errorf("failed to get torrent after repair: %w", err)
			}

			newDebridName := newTorrent.ActiveDebrid
			newPlacementFile, err := m.getPlacementFile(newTorrent, newDebridName, file.Name)
			if err != nil {
				return emptyDownloadLink, fmt.Errorf("failed to get placement file after repair: %w", err)
			}

			// Retry download link fetch
			return m.fetchDownloadLink(newTorrent, file, newPlacementFile, newDebridName)
		} else if errors.Is(err, utils.TrafficExceededError) {
			return emptyDownloadLink, err
		} else {
			return emptyDownloadLink, fmt.Errorf("failed to get download link: %w", err)
		}
	}

	if downloadLink.Empty() {
		return emptyDownloadLink, fmt.Errorf("download link is empty")
	}

	return downloadLink, nil
}

// checkDownloadLink checks if a download link is cached and valid
func (m *Manager) checkDownloadLink(link string, debridName string) (types.DownloadLink, error) {
	client := m.DebridClient(debridName)
	if client == nil {
		return types.DownloadLink{}, fmt.Errorf("debrid client not found: %s", debridName)
	}

	dl, err := client.AccountManager().GetDownloadLink(link)
	if err != nil {
		return dl, err
	}

	if m.invalidDownloadLinks != nil && !m.downloadLinkIsInvalid(dl.DownloadLink) {
		return dl, nil
	}

	return types.DownloadLink{}, fmt.Errorf("download link not found for %s", link)
}

// GetFileDownloadLinks generates download links for all files in a torrent
func (m *Manager) GetFileDownloadLinks(torrent *storage.Torrent) error {
	if torrent.ActiveDebrid == "" {
		return fmt.Errorf("no active debrid for torrent")
	}

	client := m.DebridClient(torrent.ActiveDebrid)
	if client == nil {
		return fmt.Errorf("debrid client not found: %s", torrent.ActiveDebrid)
	}

	placement := torrent.GetActivePlacement()
	if placement == nil {
		return fmt.Errorf("no active placement found")
	}

	// Build debrid torrent
	debridTorrent := &types.Torrent{
		Id:       placement.ID,
		InfoHash: torrent.InfoHash,
		Name:     torrent.Name,
		Debrid:   torrent.ActiveDebrid,
	}

	if err := client.GetFileDownloadLinks(debridTorrent); err != nil {
		return fmt.Errorf("failed to generate download links: %w", err)
	}

	return nil
}

// IncrementFailedLinkCounter increments the failure counter for a link
func (m *Manager) IncrementFailedLinkCounter(link string) int32 {
	counter, _ := m.failedLinksCounter.LoadOrCompute(link, func() (atomic.Int32, bool) {
		return atomic.Int32{}, true
	})
	return counter.Add(1)
}

// MarkLinkAsInvalid marks a download link as invalid
func (m *Manager) MarkLinkAsInvalid(downloadLink types.DownloadLink, reason string) {
	// Increment file link error counter
	m.IncrementFailedLinkCounter(downloadLink.Link)

	if m.invalidDownloadLinks != nil {
		m.invalidDownloadLinks.Store(downloadLink.DownloadLink, reason)
	}

	client := m.DebridClient(downloadLink.Debrid)
	if client == nil {
		return
	}

	accountManager := client.AccountManager()

	if reason == "bandwidth_exceeded" {
		// Disable the account
		account, err := accountManager.GetAccount(downloadLink.Token)
		if err != nil {
			m.logger.Error().Err(err).Str("token", utils.Mask(downloadLink.Token)).Msg("Failed to get account to disable")
			return
		}
		if account == nil {
			m.logger.Error().Str("token", utils.Mask(downloadLink.Token)).Msg("Account not found to disable")
			return
		}
		accountManager.Disable(account)
	} else if reason == "link_not_found" {
		// Delete the download link from the account
		account, err := accountManager.GetAccount(downloadLink.Token)
		if err != nil {
			m.logger.Error().Err(err).Str("token", utils.Mask(downloadLink.Token)).Msg("Failed to get account to delete download link")
			return
		}
		if account == nil {
			m.logger.Error().Str("token", utils.Mask(downloadLink.Token)).Msg("Account not found to delete download link")
			return
		}

		if err := client.DeleteDownloadLink(account, downloadLink); err != nil {
			m.logger.Error().Err(err).Str("token", utils.Mask(downloadLink.Token)).Msg("Failed to delete download link from account")
			return
		}
	}
}

// downloadLinkIsInvalid checks if a download link is marked as invalid
func (m *Manager) downloadLinkIsInvalid(downloadLink string) bool {
	if m.invalidDownloadLinks == nil {
		return false
	}

	if _, ok := m.invalidDownloadLinks.Load(downloadLink); ok {
		return true
	}
	return false
}

// GetDownloadByteRange gets the byte range for a file
func (m *Manager) GetDownloadByteRange(torrentName, filename string) (*[2]int64, error) {
	t, err := m.storage.GetByName(torrentName)
	if err != nil {
		return nil, fmt.Errorf("torrent not found: %w", err)
	}

	file, ok := t.Files[filename]
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
