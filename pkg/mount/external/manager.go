package external

import (
	"context"

	"github.com/sirrobot01/decypharr/pkg/manager"
)

type Manager struct{}

// NewManager creates a new external rclone manager
// This does nothing, just a placeholder to satisfy the interface
func NewManager(manager *manager.Manager) *Manager {
	return &Manager{}
}

func (m *Manager) Start(ctx context.Context) error {
	return nil
}

func (m *Manager) Stop(ctx context.Context) error {
	return nil
}

func (m *Manager) IsReady() bool {
	return true
}

func (m *Manager) Stats() map[string]interface{} {
	return map[string]interface{}{
		"enabled": true,
		"ready":   true,
		"type":    m.Type(),
	}
}

func (m *Manager) Type() string {
	return "external"
}
