package manager

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cavaliergopher/grab/v3"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	torrentpkg "github.com/sirrobot01/decypharr/pkg/storage"
)

// processDownloadAction implements actual file download from debrid
func (m *Manager) processDownloadAction(torrent *torrentpkg.Torrent, debridTorrent *debridTypes.Torrent) {
	// Get download links
	var (
		isMultiSeason bool
		seasons       []SeasonInfo
		err           error
	)
	if !torrent.SkipMultiSeason {
		isMultiSeason, seasons, err = m.detectMultiSeason(debridTorrent)
		if err != nil {
			m.logger.Warn().Msgf("Error detecting multi-season for %s: %v", debridTorrent.Name, err)
			// Continue with normal processing if detection fails
			isMultiSeason = false
		}
	}

	if isMultiSeason {
		if seasonResults, err := m.convertToMultiSeason(torrent, debridTorrent, seasons); err == nil {
			for _, result := range seasonResults {
				if err := m.queue.Add(result.torrent); err != nil {
					m.logger.Error().Err(err).Msgf("Failed to save season torrent")
					continue
				}
				// Then process the symlinks for each season torrent
				m.processDownload(result.torrent, result.debridTorrent)
			}
			// Remove the original torrent after processing seasons
			if err := m.queue.Delete(torrent.InfoHash, torrent.Category, nil); err != nil {
				m.logger.Warn().Err(err).Msg("Failed to delete original multi-season torrent")
			}
		}
	}
	m.processDownload(torrent, debridTorrent)
}

// processDownload downloads all files for a torrent with progress tracking
func (m *Manager) processDownload(torrent *torrentpkg.Torrent, debridTorrent *debridTypes.Torrent) {
	var wg sync.WaitGroup
	m.logger.Info().Msgf("Downloading %d files...", len(debridTorrent.Files))

	totalSize := int64(0)
	for _, file := range debridTorrent.GetFiles() {
		totalSize += file.Size
	}
	downloadedFolder := filepath.Join(torrent.SavePath, utils.RemoveExtension(torrent.Name))
	err := os.MkdirAll(downloadedFolder, os.ModePerm)
	if err != nil {
		return
	}

	debridTorrent.Lock()
	debridTorrent.SizeDownloaded = 0 // Reset downloaded bytes
	debridTorrent.Progress = 0       // Reset progress
	debridTorrent.Unlock()

	progressCallback := func(downloaded int64, speed int64) {
		debridTorrent.Lock()
		defer debridTorrent.Unlock()

		// Update total downloaded bytes
		debridTorrent.SizeDownloaded += downloaded
		debridTorrent.Speed = speed

		// Calculate overall progress
		if totalSize > 0 {
			debridTorrent.Progress = float64(debridTorrent.SizeDownloaded) / float64(totalSize) * 100
		}

		// Update torrent progress
		torrent.Progress = debridTorrent.Progress / 100.0
		torrent.Speed = speed
		torrent.UpdatedAt = time.Now()
		_ = m.queue.Update(torrent)
	}

	client := &grab.Client{
		UserAgent: "Decypharr[QBitTorrent]",
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
			},
		},
	}

	errChan := make(chan error, len(debridTorrent.Files))
	for _, file := range debridTorrent.GetFiles() {
		if file.DownloadLink.Empty() {
			m.logger.Info().Msgf("No download link found for %s", file.Name)
			continue
		}
		wg.Add(1)
		m.downloadSem <- struct{}{}
		go func(file debridTypes.File) {
			defer wg.Done()
			defer func() { <-m.downloadSem }()
			filename := file.Name

			err := m.grabber(
				client,
				file.DownloadLink.DownloadLink,
				filepath.Join(downloadedFolder, filename),
				file.ByteRange,
				progressCallback,
			)

			if err != nil {
				m.logger.Error().Msgf("Failed to download %s: %v", filename, err)
				errChan <- err
			} else {
				m.logger.Info().Msgf("Downloaded %s", filename)
			}
		}(file)
	}
	wg.Wait()

	close(errChan)
	var errors []error
	for err := range errChan {
		if err != nil {
			errors = append(errors, err)
		}
	}
	if len(errors) > 0 {
		m.logger.Error().Msgf("Errors occurred during download: %v", errors)
		return
	}
	m.markDownloadAsComplete(debridTorrent, torrent)
	m.logger.Info().Msgf("Downloaded all files for %s", debridTorrent.Name)
}

// processSymlinkAction creates symlinks for the torrent
func (m *Manager) processSymlinkAction(torrent *torrentpkg.Torrent, debridTorrent *debridTypes.Torrent) {
	var (
		isMultiSeason bool
		seasons       []SeasonInfo
		err           error
	)
	if !torrent.SkipMultiSeason {
		isMultiSeason, seasons, err = m.detectMultiSeason(debridTorrent)
		if err != nil {
			m.logger.Warn().Msgf("Error detecting multi-season for %s: %v", debridTorrent.Name, err)
			// Continue with normal processing if detection fails
			isMultiSeason = false
		}
	}
	torrentMountPath := m.GetTorrentMountPath(torrent)

	if isMultiSeason {

		if seasonResults, err := m.convertToMultiSeason(torrent, debridTorrent, seasons); err == nil {
			for _, result := range seasonResults {
				if err := m.queue.Add(result.torrent); err != nil {
					m.logger.Error().Err(err).Msgf("Failed to save season torrent")
					continue
				}
				// Then process the symlinks for each season torrent
				if err := m.processSymlink(result.debridTorrent, result.torrent, torrentMountPath); err != nil {
					m.logger.Error().Msgf("Failed to create symlinks for season torrent %s: %v", result.torrent.Name, err)
					m.markDownloadAsError(result.debridTorrent, result.torrent, err)
					continue
				} else {
					m.markDownloadAsComplete(result.debridTorrent, result.torrent)
				}
			}
			// Remove the original torrent after processing seasons
			if err := m.queue.Delete(torrent.InfoHash, torrent.Category, nil); err != nil {
				m.logger.Warn().Err(err).Msg("Failed to delete original multi-season torrent")
			}
		}
	}

	if err := m.processSymlink(debridTorrent, torrent, torrentMountPath); err != nil {
		m.logger.Error().Msgf("Failed to create symlinks for torrent %s: %v", torrent.Name, err)
		m.markDownloadAsError(debridTorrent, torrent, err)
		return
	}

	m.markDownloadAsComplete(debridTorrent, torrent)
}

// processSymlink creates symlinks for torrent files
func (m *Manager) processSymlink(debridTorrent *debridTypes.Torrent, torrent *torrentpkg.Torrent, mountPath string) error {
	files := debridTorrent.GetFiles()
	if len(files) == 0 {
		return fmt.Errorf("no valid files found")
	}

	m.logger.Info().Msgf("Creating symlinks for %d files ...", len(files))

	torrentSymlinkPath := filepath.Join(torrent.SavePath, utils.RemoveExtension(torrent.Name))

	// Create symlink directory
	err := os.MkdirAll(torrentSymlinkPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %s: %v", torrentSymlinkPath, err)
	}

	// Track pending files
	remainingFiles := make(map[string]debridTypes.File)
	for _, file := range files {
		remainingFiles[file.Name] = file
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(30 * time.Minute)
	filePaths := make([]string, 0, len(remainingFiles))

	var checkDirectory func(string) // Recursive function
	checkDirectory = func(dirPath string) {
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			return
		}

		for _, entry := range entries {
			entryName := entry.Name()
			fullPath := filepath.Join(dirPath, entryName)

			// Check if this matches a remaining file
			if file, exists := remainingFiles[entryName]; exists {
				fileSymlinkPath := filepath.Join(torrentSymlinkPath, file.Name)

				if err := os.Symlink(fullPath, fileSymlinkPath); err == nil || os.IsExist(err) {
					filePaths = append(filePaths, fileSymlinkPath)
					delete(remainingFiles, entryName)
					m.logger.Info().Msgf("File is ready: %s", file.Name)
				}
			} else if entry.IsDir() {
				// If not found and it's a directory, check inside
				checkDirectory(fullPath)
			}
		}
	}

	for len(remainingFiles) > 0 {
		select {
		case <-ticker.C:
			checkDirectory(mountPath)

		case <-timeout:
			m.logger.Warn().Msgf("Timeout waiting for files, %d files still pending", len(remainingFiles))
			return fmt.Errorf("timeout waiting for files: %d files still pending", len(remainingFiles))
		}
	}

	// Pre-cache files if enabled
	if !m.skipPreCache && len(filePaths) > 0 {
		go func() {
			m.logger.Debug().Msgf("Pre-caching %s", debridTorrent.Name)
			if err := utils.PreCacheFile(filePaths); err != nil {
				m.logger.Error().Msgf("Failed to pre-cache file: %s", err)
			} else {
				m.logger.Debug().Msgf("Pre-cached %d files", len(filePaths))
			}
		}()
	}

	return nil
}

// grabber downloads a file with progress callback
func (m *Manager) grabber(client *grab.Client, url, filename string, byterange *[2]int64, progressCallback func(int64, int64)) error {
	req, err := grab.NewRequest(filename, url)
	if err != nil {
		return err
	}

	// Set byte range if specified
	if byterange != nil {
		byterangeStr := fmt.Sprintf("%d-%d", byterange[0], byterange[1])
		req.HTTPRequest.Header.Set("Range", "bytes="+byterangeStr)
	}

	resp := client.Do(req)

	t := time.NewTicker(time.Second * 2)
	defer t.Stop()

	var lastReported int64
Loop:
	for {
		select {
		case <-t.C:
			current := resp.BytesComplete()
			speed := int64(resp.BytesPerSecond())
			if current != lastReported {
				if progressCallback != nil {
					progressCallback(current-lastReported, speed)
				}
				lastReported = current
			}
		case <-resp.Done:
			break Loop
		}
	}

	// Report final bytes
	if progressCallback != nil {
		progressCallback(resp.BytesComplete()-lastReported, 0)
	}

	return resp.Err()
}

func (m *Manager) markDownloadAsComplete(debridTorrent *debridTypes.Torrent, torrent *torrentpkg.Torrent) {
	// Mark as completed
	downloadedPath := filepath.Join(torrent.SavePath, utils.RemoveExtension(torrent.Name))
	torrent.MarkAsCompleted(downloadedPath)
	_ = m.queue.Update(torrent)

	// Send success notification
	go func() {
		msg := fmt.Sprintf("Download completed: %s [%s] -> %s", torrent.Name, torrent.Category, downloadedPath)
		_ = request.SendDiscordMessage("download_complete", "success", msg)
	}()

	// Send callback if configured
	if torrent.CallbackURL != "" {
		go m.sendCallback(torrent.CallbackURL, torrent, "completed", nil)
	}

	// Trigger arr refresh
	go func() {
		a := m.arr.GetOrCreate(torrent.Category)
		a.Refresh()
	}()
}

func (m *Manager) markDownloadAsError(debridTorrent *debridTypes.Torrent, torrent *torrentpkg.Torrent, err error) {
	m.logger.Error().Err(err).Str("name", torrent.Name).Msg("Failed to process action")
	torrent.MarkAsError(err)
	_ = m.queue.Update(torrent)

	// Delete from debrid on error
	go func() {
		// Get the debrid client
		if client := m.DebridClient(debridTorrent.Debrid); client != nil {
			_ = client.DeleteTorrent(debridTorrent.Id)
		}
	}()

	// Send error notification
	go func() {
		msg := fmt.Sprintf("Download failed: %s [%s] - %s", torrent.Name, torrent.Category, err.Error())
		_ = request.SendDiscordMessage("download_failed", "error", msg)
	}()

	// Send callback if configured
	if torrent.CallbackURL != "" {
		go m.sendCallback(torrent.CallbackURL, torrent, "failed", err)
	}
}

// convertToMultiSeason converts a normal torrent to a multi-season torrents
func (m *Manager) convertToMultiSeason(torrent *torrentpkg.Torrent, debridTorrent *debridTypes.Torrent, seasons []SeasonInfo) ([]seasonResult, error) {

	seasonResults := make([]seasonResult, len(seasons))

	for _, seasonInfo := range seasons {
		// Create a season-specific debrid torrent
		seasonDebridTorrent := debridTorrent.Copy()

		// Update the season torrent with season-specific data
		seasonDebridTorrent.InfoHash = seasonInfo.InfoHash
		seasonDebridTorrent.Name = seasonInfo.Name

		// Filter files to only include this season's files
		seasonFiles := make(map[string]debridTypes.File)
		size := int64(0)
		for _, file := range seasonInfo.Files {
			seasonFiles[file.Name] = file
			size += file.Size
		}
		seasonDebridTorrent.Files = seasonFiles

		// Create a season-specific managed torrent
		seasonTorrent := &torrentpkg.Torrent{
			InfoHash:     seasonInfo.InfoHash,
			Name:         seasonInfo.Name,
			Size:         size,
			Bytes:        size,
			Magnet:       torrent.Magnet,
			Category:     torrent.Category,
			SavePath:     torrent.SavePath,
			Folder:       torrent.Folder,
			Status:       debridTypes.TorrentStatusDownloading,
			ActiveDebrid: torrent.ActiveDebrid,
			Action:       torrent.Action,
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
			AddedOn:      time.Now(),
			Placements:   make(map[string]*torrentpkg.Placement),
		}

		// Copy placement
		for debridName, placement := range torrent.Placements {
			seasonTorrent.Placements[debridName] = placement
		}
		seasonResults = append(seasonResults, seasonResult{
			torrent:       seasonTorrent,
			debridTorrent: seasonDebridTorrent,
		})
	}
	return seasonResults, nil

}
