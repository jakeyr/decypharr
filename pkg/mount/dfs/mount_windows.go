//go:build windows

package dfs

import (
	"fmt"

	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/backend"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/backend/cgofuse"
)

// NewMount creates a new FUSE filesystem with the specified backend.
// On Windows, only the cgofuse backend (WinFsp) is supported.
func NewMount(mountName string, mgr *manager.Manager, backendType backend.Type) (*Mount, error) {
	if backendType == backend.Hanwen {
		return nil, fmt.Errorf("hanwen backend is not supported on Windows, use cgofuse (cgo) backend instead")
	}

	mount, vfsManager, fuseConfig, err := newMount(mountName, mgr, backendType)
	if err != nil {
		return nil, err
	}

	// Create backend-specific root node
	switch backendType {
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

// RefreshDirectory is a no-op on Windows.
// cgofuse doesn't need explicit refresh - entries are fetched dynamically.
func (m *Mount) RefreshDirectory(name string) {
	// no-op on Windows
}
