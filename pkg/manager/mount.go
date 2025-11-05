package manager

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/version"
)

type MountManager interface {
	Start(ctx context.Context) error
	Stop() error
	Stats() map[string]map[string]interface{}
	IsReady() bool
	Type() string
}

type Mount interface {
	Start(ctx context.Context) error
	Stop() error
	Refresh(dirs []string) error
	Type() string
	Stats() map[string]interface{}
}

const (
	AllFolderName      string = "__all__"
	BadFolderName      string = "__bad__"
	TorrentsFolderName string = "torrents"
)

// FileInfo implements os.FileInfo
type FileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
	content []byte
	path    string
}

func (f *FileInfo) Name() string       { return f.name }
func (f *FileInfo) Size() int64        { return f.size }
func (f *FileInfo) Mode() os.FileMode  { return f.mode }
func (f *FileInfo) ModTime() time.Time { return f.modTime }
func (f *FileInfo) IsDir() bool        { return f.isDir }
func (f *FileInfo) Sys() interface{}   { return nil }
func (f *FileInfo) Content() []byte    { return f.content }
func (f *FileInfo) Path() string       { return f.path }

// GetTorrentMountPath returns the full mount path for a torrent
// Returns the path based on the new unified mount structure
func (m *Manager) GetTorrentMountPath(torrent *storage.Torrent) string {
	debridPath := GetMountPath(torrent.ActiveDebrid)
	return filepath.Join(debridPath, torrent.Folder)
}

func (m *Manager) getAlphabeticalBucket(name string) string {
	if name == "" {
		return "UNKNOWN"
	}

	firstChar := strings.ToUpper(string(name[0]))

	switch {
	case firstChar >= "A" && firstChar <= "F":
		return "A-F"
	case firstChar >= "G" && firstChar <= "M":
		return "G-M"
	case firstChar >= "N" && firstChar <= "S":
		return "N-S"
	case firstChar >= "T" && firstChar <= "Z":
		return "T-Z"
	case firstChar >= "0" && firstChar <= "9":
		return "0-9"
	default:
		return "OTHER"
	}
}

func (m *Manager) setMountPaths() {
	cfg := config.Get()
	mountPaths := make(map[string]*FileInfo)
	baseMount := cfg.Rclone.MountPath
	if cfg.Dfs.Enabled && cfg.Dfs.MountPath != "" {
		baseMount = cfg.Dfs.MountPath
	}
	for _, dc := range cfg.Debrids {
		mountPath := dc.RcloneMountPath
		if mountPath == "" {
			mountPath = filepath.Join(baseMount, dc.Name)
		}

		mountPaths[dc.Name] = &FileInfo{
			name:    dc.Name,
			size:    0,
			modTime: time.Now(),
			isDir:   true,
			path:    mountPath,
		}
	}
	// Add manager mount path if set
	mountPaths["decypharr"] = &FileInfo{
		name:    "decypharr",
		size:    0,
		modTime: time.Now(),
		isDir:   true,
		path:    filepath.Join(baseMount, "decypharr"),
	}

	m.mountPaths = mountPaths
}

// MountPaths returns a list of parent directories used in the mount structure
// These are like /mnt/remote/realdebrid
func (m *Manager) MountPaths() (*FileInfo, []FileInfo) {
	infos := make([]FileInfo, 0, len(m.mountPaths))
	for _, mount := range m.mountPaths {
		infos = append(infos, *mount)
	}
	currentInfo := &FileInfo{
		name:    "",
		size:    0,
		modTime: time.Now(),
		isDir:   true,
	}
	return currentInfo, infos
}

func (m *Manager) GetMountInfo(name string) *FileInfo {
	mount, ok := m.mountPaths[name]
	if !ok {
		return nil
	}
	return mount
}

// GetSubDir returns the subdirectories under a given mount name
// For the mount named "realdebrid", it would show __all__, __bad__, and any custom folders
// For the new "manager" mount, it would show "torrents"
func (m *Manager) GetSubDir(subGroup string) (*FileInfo, []FileInfo) {
	var subDirs []FileInfo
	parent, ok := m.mountPaths[subGroup]
	if !ok {
		return nil, subDirs
	}
	extras := []string{AllFolderName, BadFolderName, TorrentsFolderName}
	for _, dir := range extras {
		subDirs = append(subDirs, FileInfo{
			name:    dir,
			isDir:   true,
			modTime: time.Now(),
			size:    parent.size,
		})
	}
	// Add custom folders
	if m.customFolders != nil {
		for _, folderName := range m.customFolders.folders {
			subDirs = append(subDirs, FileInfo{
				name:    folderName,
				isDir:   true,
				modTime: time.Now(),
				size:    parent.size,
			})
		}
	}

	// Add version.txt
	subDirs = append(subDirs, FileInfo{
		name:    "version.txt",
		isDir:   false,
		modTime: time.Now(),
		size:    int64(len(version.GetInfo().String())),
		content: []byte(version.GetInfo().Version),
	})
	return parent, subDirs
}

// GetChildren
// Groups are __all__, __bad__, custom folders, and paginated
func (m *Manager) GetChildren(group string) (*FileInfo, []FileInfo) {
	currentDir := &FileInfo{
		name:    group,
		size:    0,
		modTime: time.Now(),
		isDir:   true,
	}
	switch group {
	case AllFolderName, TorrentsFolderName:
		// This returns all torrents
		torrents, err := m.GetTorrents(nil)
		if err != nil {
			return nil, nil
		}
		return currentDir, m.convertTorrentsToFileInfo(torrents)
	case BadFolderName:
		torrents, err := m.GetTorrents(func(t *storage.Torrent) bool {
			return t.Bad
		})
		if err != nil {
			return nil, nil
		}
		return currentDir, m.convertTorrentsToFileInfo(torrents)
	case "version.txt":
		currentDir.content = []byte(version.GetInfo().Version)
		currentDir.size = int64(len(currentDir.content))
		currentDir.isDir = false
		return currentDir, nil
	default:
		// Custom folder
		return currentDir, m.getCustomFolderChildren(group)
	}
}

func (m *Manager) GetChildrenInSubGroup(subGroup string) (*FileInfo, []FileInfo) {
	currentDir := &FileInfo{
		name:    subGroup,
		size:    0,
		modTime: time.Now(),
		isDir:   true,
	}

	// Paginated buckets
	switch subGroup {
	case "A-F", "G-M", "N-S", "T-Z", "0-9", "OTHER":
		torrents, err := m.GetTorrents(func(t *storage.Torrent) bool {
			if t.Bad {
				return false
			}
			bucket := m.getAlphabeticalBucket(t.Folder)
			return bucket == subGroup
		})
		if err != nil {
			return nil, nil
		}
		return currentDir, m.convertTorrentsToFileInfo(torrents)
	default:
		return nil, nil
	}

}

func (m *Manager) GetTorrentFilesInFolder(group, name string) (*FileInfo, []FileInfo) {
	// Find the torrent by folder name
	torr, err := m.GetTorrentByName(name)
	if err != nil || torr == nil {
		return nil, nil
	}
	if group == BadFolderName && !torr.Bad {
		return nil, nil
	}

	// Convert files to FileInfo
	infos := make([]FileInfo, 0, len(torr.Files))
	for _, file := range torr.Files {
		infos = append(infos, FileInfo{
			name:    file.Name,
			size:    file.Size,
			modTime: torr.AddedOn,
			isDir:   false,
		})
	}

	currentDir := &FileInfo{
		name:    torr.Folder,
		size:    torr.Size,
		modTime: torr.AddedOn,
		isDir:   true,
	}
	return currentDir, infos
}

func (m *Manager) getPaginatedInfo() []FileInfo {
	buckets := make([]FileInfo, 0)
	bucketNames := []string{"A-F", "G-M", "N-S", "T-Z", "0-9", "OTHER"}

	for _, bucket := range bucketNames {
		buckets = append(buckets, FileInfo{
			name:    bucket,
			size:    0,
			modTime: time.Now(),
			isDir:   true,
		})
	}
	return buckets
}

func (m *Manager) convertTorrentsToFileInfo(torrents []*storage.Torrent) []FileInfo {
	infos := make([]FileInfo, 0, len(torrents))
	for _, t := range torrents {
		infos = append(infos, FileInfo{
			name:    t.Folder,
			size:    t.Size,
			modTime: t.AddedOn,
			isDir:   true,
		})
	}
	return infos
}

func (m *Manager) getCustomFolderChildren(folder string) []FileInfo {
	filters := m.customFolders.filters[folder]
	if len(filters) == 0 {
		return nil
	}

	torrents, err := m.GetTorrents(func(t *storage.Torrent) bool {
		if t.Bad {
			return false
		}
		return m.customFolders.matchesFilter(folder, &FileInfo{
			name: t.Folder,
			size: t.Size,
		}, t.AddedOn)
	})
	if err != nil {
		return nil
	}
	return m.convertTorrentsToFileInfo(torrents)
}

func GetMountPath(debridName string) string {
	cfg := config.Get()

	var baseMount string
	if cfg.Rclone.Enabled {
		baseMount = cfg.Rclone.MountPath
	}
	if cfg.Dfs.Enabled && cfg.Dfs.MountPath != "" {
		baseMount = cfg.Dfs.MountPath
	}
	var dc config.Debrid
	for _, d := range cfg.Debrids {
		if d.Name == debridName {
			dc = d
			break
		}
	}
	if dc.Name == "" && debridName != "decypharr" {
		// decypharr is the manager mount
		return ""
	}

	mountPath := dc.RcloneMountPath
	if dc.RcloneMountPath == "" {
		mountPath = filepath.Join(baseMount, dc.Name) // dc.Name will be "" for manager mount since we're mounting directly in that folder
	}
	return mountPath
}
