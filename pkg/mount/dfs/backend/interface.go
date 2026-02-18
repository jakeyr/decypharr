package backend

import (
	"cmp"
	"context"
	"os"
	"runtime"

	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs"
)

// Type represents the type of FUSE backend
type Type string

const (
	Hanwen Type = "hanwen"
	Cgo    Type = "cgo"
)

// DefaultBackend returns the recommended backend for the current platform
// Linux: hanwen (fastest, pure Go)
// macOS/Windows: cgofuse (cross-platform, works with Fuse-T/WinFsp)
func DefaultBackend() Type {
	if runtime.GOOS == "linux" {
		// Get from environment variable override
		backendEnv := cmp.Or(os.Getenv("DFS_FUSE_BACKEND"), "hanwen")
		switch backendEnv {
		case "hanwen":
			return Hanwen
		case "cgo":
			return Cgo
		default:
			return Hanwen
		}
	}
	return Cgo
}

// Backend represents a FUSE backend implementation
type Backend interface {
	// Mount mounts the filesystem at the configured path
	Mount(ctx context.Context, root RootNode) error

	// Unmount unmounts the filesystem
	Unmount(ctx context.Context) error

	// WaitReady waits for the mount to be ready
	WaitReady(ctx context.Context) error

	// IsReady returns true if the mount is ready
	IsReady() bool

	// Type returns the backend type
	Type() Type
}

// RootNode represents the root node of the filesystem that backends will mount
type RootNode interface {
	// GetVFS returns the VFS manager
	GetVFS() *vfs.Manager

	// GetConfig returns the FUSE configuration
	GetConfig() *config.FuseConfig

	// GetManager returns the manager
	GetManager() *manager.Manager

	// GetRootDir returns the root directory implementation
	GetRootDir() interface{}
}

// Factory creates a new backend instance
type Factory func(config *config.FuseConfig) (Backend, error)

var factories = make(map[Type]Factory)

// Register registers a backend factory
func Register(backendType Type, factory Factory) {
	factories[backendType] = factory
}

// Create creates a new backend of the specified type
func Create(backendType Type, config *config.FuseConfig) (Backend, error) {
	factory, ok := factories[backendType]
	if !ok {
		// Default to platform default if not found
		factory = factories[DefaultBackend()]
	}
	return factory(config)
}
