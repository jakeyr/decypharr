package manager

import (
	"context"
	"strings"
)

type Event struct {
	OnRefresh func(dirs []string) error
	OnStart   func(ctx context.Context) error
	OnStop    func() error
}

func (m *Manager) SetEventHandlers(e *Event) {
	m.event = e
}

func (m *Manager) TriggerRefreshEvent(refreshMount bool) {

	dirs := strings.FieldsFunc(m.config.RefreshDirs, func(r rune) bool {
		return r == ',' || r == '&'
	})
	if len(dirs) == 0 {
		dirs = []string{"__all__"}
	}
	if refreshMount {
		if m.event != nil && m.event.OnRefresh != nil {
			err := m.event.OnRefresh(dirs)
			if err != nil {
				m.logger.Error().Err(err).Msg("Failed to refresh mount")
			}
		}
	}
}

func (m *Manager) TriggerStartEvent(ctx context.Context) error {
	if m.event != nil && m.event.OnStart != nil {
		return m.event.OnStart(ctx)
	}
	return nil
}

func (m *Manager) TriggerStopEvent() error {
	if m.event != nil && m.event.OnStop != nil {
		return m.event.OnStop()
	}
	return nil
}

func NewEventHandlers(mounter Mount) *Event {
	return &Event{
		OnStart:   mounter.Start,
		OnStop:    mounter.Stop,
		OnRefresh: mounter.Refresh,
	}
}
