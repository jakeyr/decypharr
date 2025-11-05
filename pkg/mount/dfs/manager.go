package dfs

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

// Manager manages FUSE filesystem instances with proper caching
type Manager struct {
	mounts  map[string]*Mount
	manager *manager.Manager
	logger  zerolog.Logger
	mu      sync.RWMutex
	ready   atomic.Bool
}

// NewManager creates a new  FUSE filesystem manager
func NewManager(manager *manager.Manager) *Manager {
	m := &Manager{
		manager: manager,
		logger:  logger.New("mount"),
	}
	m.registerMounts()
	return m
}

func (m *Manager) registerMounts() {
	mounts := make(map[string]*Mount)
	_, mountPaths := m.manager.MountPaths()
	for _, mountInfo := range mountPaths {
		mnt, err := NewMount(mountInfo, m.manager)
		if err != nil {
			m.logger.Error().Err(err).Msgf("Failed to create FUSE mount for debrid: %s", mountInfo.Name())
			continue
		}
		mounts[mountInfo.Name()] = mnt
	}
	m.mu.Lock()
	m.mounts = mounts
	m.mu.Unlock()
}

// Start starts the FUSE filesystem manager
func (m *Manager) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	m.mu.RLock()
	defer m.mu.RUnlock()

	for name, mount := range m.mounts {
		wg.Add(1)
		go func(name string, mount *Mount) {
			defer wg.Done()
			if err := mount.Start(ctx); err != nil {
				m.logger.Error().Err(err).Msgf("Failed to mount FUSE filesystem for debrid: %s", name)
			} else {
				m.logger.Info().Msgf("Successfully mounted FUSE filesystem for debrid: %s", name)
			}
		}(name, mount)
	}
	wg.Wait()
	m.ready.Store(true)
	return nil
}

// Stop stops the  FUSE filesystem manager
func (m *Manager) Stop() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for name, mount := range m.mounts {
		if err := mount.Stop(); err != nil {
			m.logger.Error().Err(err).Msgf("Failed to unmount FUSE filesystem for debrid: %s", name)
		} else {
			m.logger.Info().Msgf("Successfully unmounted FUSE filesystem for debrid: %s", name)
		}
	}
	return nil
}

func (m *Manager) IsReady() bool {
	return m.ready.Load()
}

func (m *Manager) GetStats() (map[string]interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := map[string]interface{}{
		"enabled": true,
		"ready":   true,
	}

	// Collect stats from all registered mounts
	if len(m.mounts) > 0 {
		mountsInfo := make(map[string]interface{})

		for name, mount := range m.mounts {
			mountStats := mount.Stats()
			if mountStats != nil {
				mountsInfo[name] = mountStats
			}
		}

		stats["mounts"] = mountsInfo
	}

	return stats, nil
}
