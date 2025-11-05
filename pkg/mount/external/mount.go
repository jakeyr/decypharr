package external

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
)

type Mount struct {
	config     config.Debrid
	httpClient *http.Client
	logger     zerolog.Logger
}

func NewMount(debridConfig config.Debrid) (*Mount, error) {
	return &Mount{
		config:     debridConfig,
		httpClient: http.DefaultClient,
		logger:     logger.New("rclone-external"),
	}, nil
}

func (m *Mount) Start(ctx context.Context) error {
	return nil
}

func (m *Mount) Stats() map[string]interface{} {
	return map[string]interface{}{
		"enabled": true,
		"ready":   true,
		"type":    m.Type(),
	}
}

func (m *Mount) Stop() error {
	return nil
}

func (m *Mount) Refresh(dirs []string) error {
	return m.refresh(dirs)
}

func (m *Mount) refresh(dirs []string) error {
	cfg := m.config

	if cfg.RcUrl == "" {
		return nil
	}
	// Create form data
	data := m.buildRcloneRequestData(dirs)

	if err := m.sendRcloneRequest("vfs/forget", data); err != nil {
		m.logger.Error().Err(err).Msg("Failed to send rclone vfs/forget request")
	}

	if err := m.sendRcloneRequest("vfs/refresh", data); err != nil {
		m.logger.Error().Err(err).Msg("Failed to send rclone vfs/refresh request")
	}

	return nil
}

func (m *Mount) buildRcloneRequestData(dirs []string) string {
	var data strings.Builder
	for index, dir := range dirs {
		if dir != "" {
			if index == 0 {
				data.WriteString("dir=" + dir)
			} else {
				data.WriteString("&dir" + fmt.Sprint(index+1) + "=" + dir)
			}
		}
	}
	return data.String()
}

func (m *Mount) sendRcloneRequest(endpoint, data string) error {
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/%s", m.config.RcUrl, endpoint), strings.NewReader(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	if m.config.RcUser != "" && m.config.RcPass != "" {
		req.SetBasicAuth(m.config.RcUser, m.config.RcPass)
	}
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("failed to perform %s: %s - %s", endpoint, resp.Status, string(body))
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (m *Mount) Type() string {
	return "rcloneExternal"
}
