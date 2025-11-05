package manager

import (
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Migrator returns the migrator instance
func (m *Manager) Migrator() *Migrator {
	return m.migrator
}

// Arr returns the Arr storage instance
func (m *Manager) Arr() *arr.Storage {
	return m.arr
}

func (m *Manager) Queue() *Queue {
	return m.queue
}

func (m *Manager) Clients() *xsync.Map[string, debrid.Client] {
	return m.clients
}

func (m *Manager) MountManager() MountManager {
	return m.mountManager
}

func (m *Manager) Storage() *storage.Storage {
	return m.storage
}
