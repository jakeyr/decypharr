package manager

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cavaliergopher/grab/v3"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

type Downloader struct {
	manager     *Manager
	strmURL     string
	mountPath   string
	dest        string
	logger      zerolog.Logger
	downloadSem chan struct{}
}

// NewDownloadManager creates a new strm manager
func NewDownloadManager(manager *Manager) *Downloader {
	cfg := config.Get()
	strmURL := cfg.AppURL
	if strmURL == "" {
		bindAddress := cfg.BindAddress
		if bindAddress == "" {
			bindAddress = "localhost"
		}

		strmURL = fmt.Sprintf("http://%s:%s", bindAddress, cfg.Port)
	}
	return &Downloader{
		manager:     manager,
		strmURL:     strmURL,
		mountPath:   cfg.Mount.MountPath,
		logger:      manager.logger.With().Str("component", "downloader").Logger(),
		dest:        cfg.DownloadFolder,
		downloadSem: make(chan struct{}, cfg.MaxDownloads),
	}
}

func (d *Downloader) download(torrent *storage.Entry) error {
	var (
		isMultiSeason bool
		seasons       []SeasonInfo
	)
	if !torrent.SkipMultiSeason {
		isMultiSeason, seasons = d.detectMultiSeason(torrent)
	}
	torrentMountPath := d.manager.GetTorrentMountPath(torrent)
	if isMultiSeason {

		seasonResults := convertToMultiSeason(torrent, seasons)
		for _, result := range seasonResults {
			if err := d.manager.queue.Add(result); err != nil {
				d.logger.Error().Err(err).Msgf("Failed to save season torrent")
				continue
			}
			// Then process the symlinks for each season torrent
			if err := d.process(result, torrentMountPath); err != nil {
				d.markAsError(result, err)
			}
		}
	}
	return d.process(torrent, torrentMountPath)
}

func (d *Downloader) process(entry *storage.Entry, mountPath string) error {
	switch entry.Action {
	case config.DownloadActionDownload:
		return d.processDownload(entry)
	case config.DownloadActionSymlink:
		return d.processSymlink(entry, mountPath)
	case config.DownloadActionStrm:
		return d.processStrm(entry)
	case config.DownloadActionNone:
		d.markAsCompleted(entry)
		// Remove entry from queue
		_ = d.manager.queue.Delete(entry.InfoHash, nil)
		return nil
	default:
		return d.processSymlink(entry, mountPath)
	}
}

func (d *Downloader) markAsCompleted(entry *storage.Entry) {
	// Mark as completed
	entry.MarkAsCompleted(entry.SymlinkPath())
	_ = d.manager.queue.Update(entry)

	// Send notification
	msg := fmt.Sprintf("Download completed: %s [%s] -> %s", entry.Name, entry.Category, entry.SymlinkPath())
	d.manager.Notifications.Notify(notifications.Event{
		Type:    config.EventDownloadComplete,
		Status:  "success",
		Entry:   entry,
		Message: msg,
	})

	// Trigger arr refresh
	go func() {
		a := d.manager.arr.GetOrCreate(entry.Category)
		a.Refresh()
	}()
}

func (d *Downloader) markAsError(entry *storage.Entry, err error) {
	d.logger.Error().Err(err).Str("name", entry.Name).Msg("Failed to process action")
	entry.MarkAsError(err)
	_ = d.manager.queue.Update(entry)

	// Send error notification
	msg := fmt.Sprintf("Download failed: %s [%s] - %s", entry.Name, entry.Category, err.Error())
	d.manager.Notifications.Notify(notifications.Event{
		Type:    config.EventDownloadFailed,
		Status:  "error",
		Entry:   entry,
		Message: msg,
		Error:   err,
	})
}

// processSymlink creates symlinks for torrent files
func (d *Downloader) processSymlink(entry *storage.Entry, mountPath string) error {
	files := entry.GetActiveFiles()
	torrentSymlinkPath := entry.SymlinkPath()
	d.logger.Info().Str("mount_path", mountPath).Msgf("Creating symlinks for %d files in %s", len(files), torrentSymlinkPath)

	// Create symlink directory
	err := os.MkdirAll(torrentSymlinkPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %s: %v", torrentSymlinkPath, err)
	}

	// Track pending files
	remainingFiles := make(map[string]*storage.File)
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

		for _, item := range entries {
			entryName := item.Name()
			fullPath := filepath.Join(dirPath, entryName)

			// Check if this matches a remaining file
			if file, exists := remainingFiles[entryName]; exists {
				fileSymlinkPath := filepath.Join(torrentSymlinkPath, file.Name)

				if err := os.Symlink(fullPath, fileSymlinkPath); err == nil || os.IsExist(err) {
					filePaths = append(filePaths, fileSymlinkPath)
					delete(remainingFiles, entryName)
					d.logger.Info().Msgf("File is ready: %s/%s", entry.GetFolder(), file.Name)
				}
			} else if item.IsDir() {
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
			return fmt.Errorf("timeout waiting for files: %d files still pending", len(remainingFiles))
		}
	}
	d.markAsCompleted(entry)

	// Run ffprobe on files to warm cache and trigger imports
	if !d.manager.config.SkipPreCache && len(filePaths) > 0 {
		go func() {
			d.logger.Debug().Msgf("Running ffprobe on %s", entry.Name)
			if err := d.manager.RunFFprobe(filePaths); err != nil {
				d.logger.Error().Msgf("Failed to run ffprobe: %s", err)
			} else {
				d.logger.Debug().Str("entry", entry.Name).Msgf("Ran ffprobe on %d files", len(filePaths))
			}
		}()
	}

	return nil
}

// processDownload downloads all files for an entry with progress tracking
// For torrents: uses HTTP download from debrid
// For NZBs: uses parallel NNTP segment download
func (d *Downloader) processDownload(entry *storage.Entry) error {
	// Check if this is a usenet entry
	if entry.IsNZB() {
		return d.processUsenetDownload(entry)
	}
	return d.processTorrentDownload(entry)
}

// processTorrentDownload downloads files from debrid via HTTP
func (d *Downloader) processTorrentDownload(entry *storage.Entry) error {
	var wg sync.WaitGroup
	files := entry.GetActiveFiles()
	d.logger.Info().Msgf("Downloading %d files...", len(files))

	totalSize := int64(0)
	for _, file := range files {
		totalSize += file.Size
	}
	downloadedFolder := entry.SymlinkPath()
	err := os.MkdirAll(downloadedFolder, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create download directory: %s: %v", downloadedFolder, err)
	}
	entry.SizeDownloaded = 0 // Reset downloaded bytes
	entry.Progress = 0       // Reset progress

	progressCallback := func(downloaded int64, speed int64) {
		var mu sync.Mutex
		mu.Lock()
		defer mu.Unlock()

		// Update total downloaded bytes
		entry.SizeDownloaded += downloaded
		entry.Speed = speed

		// Calculate overall progress
		if totalSize > 0 {
			entry.Progress = float64(entry.SizeDownloaded) / float64(totalSize) * 100
		}

		// Update entry progress
		entry.Progress = entry.Progress / 100.0
		entry.Speed = speed
		entry.UpdatedAt = time.Now()
		_ = d.manager.queue.Update(entry)
	}

	errChan := make(chan error, len(files))
	for _, file := range files {
		downloadLink, err := d.manager.linkService.GetLink(context.Background(), entry, file.Name)
		if err != nil {
			d.logger.Error().Msgf("Failed to get download link for %s: %v", file.Name, err)
			continue
		}
		client := &grab.Client{
			UserAgent: "Decypharr[QBitTorrent]",
			HTTPClient: &http.Client{
				Transport: &http.Transport{
					Proxy: http.ProxyFromEnvironment,
				},
			},
		}
		wg.Add(1)
		d.downloadSem <- struct{}{}
		go func(file *storage.File) {
			defer wg.Done()
			defer func() { <-d.downloadSem }()
			filename := file.Name

			err := d.localDownloader(
				client,
				downloadLink.DownloadLink,
				filepath.Join(downloadedFolder, filename),
				file.ByteRange,
				progressCallback,
			)

			if err != nil {
				d.logger.Error().Msgf("Failed to download %s: %v", filename, err)
				errChan <- err
			} else {
				d.logger.Info().Msgf("Downloaded %s", filename)
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
		return fmt.Errorf("errors occurred during download")
	}
	d.markAsCompleted(entry)
	d.logger.Info().Msgf("Downloaded all files for %s", entry.Name)
	return nil
}

// processUsenetDownload downloads NZB files via parallel NNTP segment fetching
func (d *Downloader) processUsenetDownload(entry *storage.Entry) error {
	if d.manager.usenet == nil {
		return fmt.Errorf("usenet client not configured")
	}

	files := entry.GetActiveFiles()
	d.logger.Info().Msgf("Downloading %d NZB files via usenet...", len(files))

	downloadedFolder := entry.SymlinkPath()
	if err := os.MkdirAll(downloadedFolder, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create download directory: %s: %v", downloadedFolder, err)
	}

	// Calculate total size for progress tracking
	totalSize := int64(0)
	for _, file := range files {
		totalSize += file.Size
	}

	entry.SizeDownloaded = 0
	entry.Progress = 0
	entry.IsDownloading = true
	_ = d.manager.queue.Update(entry)

	var wg sync.WaitGroup
	var progressMu sync.Mutex
	errChan := make(chan error, len(files))

	for _, file := range files {
		wg.Add(1)
		d.downloadSem <- struct{}{}
		go func(file *storage.File) {
			defer wg.Done()
			defer func() { <-d.downloadSem }()

			// Create destination file
			destPath := filepath.Join(downloadedFolder, file.Name)
			destFile, err := os.Create(destPath)
			if err != nil {
				errChan <- fmt.Errorf("failed to create file %s: %w", file.Name, err)
				return
			}
			defer destFile.Close()

			ctx := d.manager.ctx

			// Progress callback for this file
			progressCallback := func(downloaded int64, speed int64) {
				progressMu.Lock()
				defer progressMu.Unlock()

				entry.SizeDownloaded += downloaded - entry.SizeDownloaded
				entry.Speed = speed

				if totalSize > 0 {
					entry.Progress = float64(entry.SizeDownloaded) / float64(totalSize)
				}

				entry.UpdatedAt = time.Now()
				_ = d.manager.queue.Update(entry)
			}

			if err := d.manager.usenet.Download(ctx, entry.InfoHash, file.Name, destFile, progressCallback); err != nil {
				_ = os.Remove(destPath) // Clean up partial file
				errChan <- fmt.Errorf("failed to download %s: %w", file.Name, err)
				return
			}

			d.logger.Info().Msgf("Downloaded NZB file: %s", file.Name)
		}(file)
	}
	wg.Wait()
	entry.IsDownloading = false

	close(errChan)
	var errors []error
	for err := range errChan {
		if err != nil {
			errors = append(errors, err)
		}
	}
	if len(errors) > 0 {
		entry.MarkAsError(errors[0])
		_ = d.manager.queue.Update(entry)
		return fmt.Errorf("errors occurred during NZB download: %d files failed", len(errors))
	}

	d.markAsCompleted(entry)
	d.logger.Info().Msgf("Downloaded all NZB files for %s", entry.Name)
	return nil
}

// processStrm creates symlinks for torrent files
func (d *Downloader) processStrm(torrent *storage.Entry) error {

	files := torrent.GetActiveFiles()
	d.logger.Info().Msgf("Creating .strm for %d files ...", len(files))

	torrentSymlinkPath := torrent.SymlinkPath()

	// Create symlink directory
	err := os.MkdirAll(torrentSymlinkPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %s: %v", torrentSymlinkPath, err)
	}

	for _, file := range files {
		strmFilePath := filepath.Join(torrentSymlinkPath, file.Name+".strm")
		streamURL, err := url.JoinPath(
			d.strmURL,
			"webdav",
			"stream",
			EntryAllFolder,
			url.PathEscape(torrent.GetFolder()),
			url.PathEscape(file.Name),
		)
		if err != nil {
			continue
		}
		if err := os.WriteFile(strmFilePath, []byte(streamURL), 0644); err != nil {
			return fmt.Errorf("failed to create .strm file: %s: %v", strmFilePath, err)
		}
	}
	d.markAsCompleted(torrent)
	d.logger.Info().Str("destination", torrentSymlinkPath).Msgf("Created .strm files for %s", torrent.Name)
	return nil
}

func (d *Downloader) detectMultiSeason(torrent *storage.Entry) (bool, []SeasonInfo) {
	torrentName := torrent.Name
	files := torrent.GetActiveFiles()

	// Find all seasons present in the files
	seasonsFound := findAllSeasons(files)

	// Check if this is actually a multi-season torrent
	isMultiSeason := len(seasonsFound) > 1 || hasMultiSeasonIndicators(torrentName)

	if !isMultiSeason {
		return false, nil
	}

	d.logger.Info().Msgf("Multi-season torrent detected with seasons: %v", getSortedSeasons(seasonsFound))

	// Group files by season
	seasonGroups := groupFilesBySeason(files, seasonsFound)

	// Create SeasonInfo objects with proper naming
	var seasons []SeasonInfo
	for seasonNum, seasonFiles := range seasonGroups {
		if len(seasonFiles) == 0 {
			continue
		}

		// Generate season-specific name preserving all metadata
		seasonName := replaceMultiSeasonPattern(torrentName, seasonNum)

		seasons = append(seasons, SeasonInfo{
			SeasonNumber: seasonNum,
			Files:        seasonFiles,
			InfoHash:     generateSeasonHash(torrent.InfoHash, seasonNum),
			Name:         seasonName,
		})
	}

	return true, seasons
}

// grabber downloads a file with progress callback
func (d *Downloader) localDownloader(client *grab.Client, url, filename string, byterange *[2]int64, progressCallback func(int64, int64)) error {
	req, err := grab.NewRequest(filename, url)
	req.NoCreateDirectories = true
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
