//go:build !windows

package dfs

import (
	"fmt"
	"time"

	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/backend"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/backend/cgofuse"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/backend/hanwen"
)

// NewMount creates a new FUSE filesystem with the specified backend
func NewMount(mountName string, mgr *manager.Manager, backendType backend.Type) (*Mount, error) {
	mount, vfsManager, fuseConfig, err := newMount(mountName, mgr, backendType)
	if err != nil {
		return nil, err
	}

	// Create backend-specific root node
	now := time.Now()
	switch backendType {
	case backend.Hanwen:
		mount.rootNode = hanwen.NewDir(vfsManager, mgr, "", hanwen.LevelRoot, uint64(now.Unix()), fuseConfig, mount.logger)
	case backend.Cgo:
		mount.rootNode = cgofuse.NewFS(vfsManager, mgr, fuseConfig, mount.logger)
	default:
		return nil, fmt.Errorf("unknown backend type: %s", backendType)
	}

	// Create the backend
	bck, err := backend.Create(backendType, fuseConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create backend: %w", err)
	}
	mount.backend = bck

	return mount, nil
}

// RefreshDirectory refreshes a directory in the filesystem
func (m *Mount) RefreshDirectory(name string) {
	switch m.backendType {
	case backend.Hanwen:
		if rootDir, ok := m.rootNode.(*hanwen.Dir); ok {
			m.refreshDirectoryHanwen(rootDir, name)
		}
	case backend.Cgo:
		// cgofuse doesn't need explicit refresh - entries are fetched dynamically
	}
}

func (m *Mount) refreshDirectoryHanwen(rootDir *hanwen.Dir, name string) {
	// Always refresh the root to pick up new top-level entries
	rootDir.Refresh()

	// If a specific directory name is provided, refresh its children too
	if name != "" {
		rootDir.RefreshChild(name)
	}
}
