package vfs

import (
	"context"
	"sync"

	mainconfig "github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/lightning"
)

// LightningIntegration manages Lightning streamers
type LightningIntegration struct {
	streamer *lightning.Streamer // Single global streamer
	mu       sync.RWMutex
	enabled  bool
	manager  *manager.Manager
}

// NewLightningIntegration creates a new Lightning integration
func NewLightningIntegration(mgr *manager.Manager, fuseConfig *config.FuseConfig) (*LightningIntegration, error) {
	li := &LightningIntegration{
		enabled: true, // Enable by default
		manager: mgr,
	}

	// Create a single Lightning streamer
	// We'll use a generic base URL - Lightning will use the actual download URLs from FileInfo
	ctx := context.Background()

	// Create Lightning config based on FUSE config
	lightningConfig := convertToLightningConfig(fuseConfig)

	// Create streamer with empty base URL (we'll use full URLs from download links)
	streamer, err := lightning.NewLightningStreamer(ctx, lightningConfig, "")
	if err != nil {
		// Lightning failed to initialize - disable it
		li.enabled = false
		return li, nil
	}

	li.streamer = streamer
	return li, nil
}

// convertToLightningConfig converts FUSE config to Lightning config
func convertToLightningConfig(fuseConfig *config.FuseConfig) *lightning.Config {
	lightningConfig := lightning.DefaultConfig()

	// Map chunk size
	if fuseConfig.ChunkSize > 0 {
		lightningConfig.ChunkSize = fuseConfig.ChunkSize
	}

	// Map disk cache directory
	lightningConfig.DiskCacheDir = fuseConfig.CacheDir

	// Map disk cache size
	if fuseConfig.CacheDiskSize > 0 {
		lightningConfig.DiskCacheSize = fuseConfig.CacheDiskSize
	}

	// Map readahead size
	if fuseConfig.ReadAheadSize > 0 {
		lightningConfig.MaxReadahead = fuseConfig.ReadAheadSize
		lightningConfig.InitialReadahead = fuseConfig.ReadAheadSize / 2
	}

	// Map buffer size to memory cache
	if fuseConfig.BufferSize > 0 {
		lightningConfig.MemCacheSize = fuseConfig.BufferSize
	}

	// Adjust pool size based on concurrent reads setting
	if fuseConfig.MaxConcurrentReads > 0 {
		lightningConfig.PoolSize = fuseConfig.MaxConcurrentReads * 2 // 2 connections per concurrent read
		if lightningConfig.PoolSize > 16 {
			lightningConfig.PoolSize = 16 // Cap at 16
		}
	}

	return lightningConfig
}

// ReadAt performs a Lightning-accelerated read if available
func (li *LightningIntegration) ReadAt(ctx context.Context, fileInfo *manager.FileInfo, p []byte, offset int64) (int, bool, error) {
	if !li.enabled || li.streamer == nil {
		return 0, false, nil
	}

	// TODO: Extract download URL from fileInfo or manager
	// You need to implement getDownloadURL() based on your debrid client/manager
	downloadURL := li.getDownloadURL(ctx, fileInfo)
	if downloadURL == "" {
		// No URL available - fall back to traditional method
		return 0, false, nil
	}

	// Generate unique file ID from parent and name
	fileID := sanitizeFileID(fileInfo.Parent(), fileInfo.Name())
	fileSize := fileInfo.Size()

	// Open file in Lightning (or get existing stream)
	_, err := li.streamer.OpenFile(fileID, fileSize, downloadURL)
	if err != nil {
		// Failed to open in Lightning - fall back
		return 0, false, nil
	}

	// Perform Lightning read
	n, err := li.streamer.ReadAt(ctx, fileID, p, offset)
	if err != nil {
		// Lightning read failed - fall back to traditional method
		return 0, false, nil
	}

	// Success! Lightning handled the read
	return n, true, nil
}

// getDownloadURL extracts the download URL from fileInfo
// TODO: Implement this based on your debrid client/manager structure
func (li *LightningIntegration) getDownloadURL(ctx context.Context, fileInfo *manager.FileInfo) string {
	cfg := mainconfig.Get()
	torrent, err := li.manager.GetTorrentByName(fileInfo.Parent())
	if err != nil {
		return ""
	}
	downloadLink, err := li.manager.GetDownloadLink(torrent, fileInfo.Name())
	if err != nil {
		if cfg.SkipAutoMove {
			return ""
		}

		moveErr := li.manager.MoveTorrent(ctx, torrent)
		if moveErr != nil {
			return ""
		}
		// After moving, try to get the download link again
		// This will try to get the link from the new debrid service
		torrent, err = li.manager.GetTorrentByName(fileInfo.Parent())
		if err != nil {
			return ""
		}
		downloadLink, err = li.manager.GetDownloadLink(torrent, fileInfo.Name())
		if err != nil {
			return ""
		}
	}
	return downloadLink.DownloadLink
}

// sanitizeFileID creates a safe file ID from parent and name
func sanitizeFileID(parent, name string) string {
	if parent == "" {
		return sanitizeForPath(name)
	}
	return sanitizeForPath(parent) + "/" + sanitizeForPath(name)
}

// CloseFile closes a file in Lightning
func (li *LightningIntegration) CloseFile(fileID string) {
	if li.streamer != nil && li.enabled {
		_ = li.streamer.CloseFile(fileID)
	}
}

// GetStats returns Lightning statistics
func (li *LightningIntegration) GetStats() map[string]interface{} {
	if !li.enabled || li.streamer == nil {
		return map[string]interface{}{
			"enabled": false,
		}
	}

	return map[string]interface{}{
		"enabled": true,
		"stats":   li.streamer.GetStats(),
	}
}

// Close closes the Lightning streamer
func (li *LightningIntegration) Close() error {
	li.mu.Lock()
	defer li.mu.Unlock()

	if li.streamer != nil {
		_ = li.streamer.Close()
	}

	return nil
}

// Enable enables Lightning integration
func (li *LightningIntegration) Enable() {
	li.enabled = true
}

// Disable disables Lightning integration (falls back to traditional method)
func (li *LightningIntegration) Disable() {
	li.enabled = false
}

// IsEnabled returns whether Lightning is enabled
func (li *LightningIntegration) IsEnabled() bool {
	return li.enabled
}
