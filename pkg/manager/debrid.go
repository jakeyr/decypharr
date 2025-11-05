package manager

import (
	"cmp"
	"context"
	"errors"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/providers/alldebrid"
	"github.com/sirrobot01/decypharr/pkg/debrid/providers/debridlink"
	"github.com/sirrobot01/decypharr/pkg/debrid/providers/realdebrid"
	"github.com/sirrobot01/decypharr/pkg/debrid/providers/torbox"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"go.uber.org/ratelimit"
)

var (
	ErrUnsupportedDebridProvider = errors.New("unsupported debrid provider")
)

func (m *Manager) DebridClient(name string) debrid.Client {
	client, ok := m.clients.Load(name)
	if !ok {
		return nil
	}
	return client
}

func (m *Manager) initDebridClients() {
	cfg := config.Get()
	for _, dc := range cfg.Debrids {
		client, err := m.createClient(dc)
		if err != nil {
			m.logger.Error().Err(err).Str("debrid", dc.Name).Msg("Failed to create debrid client")
			continue
		}
		m.clients.Store(dc.Name, client)
		m.logger.Info().Str("debrid", dc.Name).Msg("Debrid client initialized")
	}
}

// createClient creates a debrid client based on configuration
func (m *Manager) createClient(dc config.Debrid) (debrid.Client, error) {
	var client debrid.Client
	var err error

	rateLimits := map[string]ratelimit.Limiter{}

	mainRL := request.ParseRateLimit(dc.RateLimit)
	repairRL := request.ParseRateLimit(cmp.Or(dc.RepairRateLimit, dc.RateLimit))
	downloadRL := request.ParseRateLimit(cmp.Or(dc.DownloadRateLimit, dc.RateLimit))

	rateLimits["main"] = mainRL
	rateLimits["repair"] = repairRL
	rateLimits["download"] = downloadRL

	switch dc.Provider {
	case "realdebrid":
		client, err = realdebrid.New(dc, rateLimits)
	case "alldebrid":
		client, err = alldebrid.New(dc, rateLimits)
	case "torbox":
		client, err = torbox.New(dc, rateLimits)
	case "debridlink":
		client, err = debridlink.New(dc, rateLimits)
	default:
		return nil, ErrUnsupportedDebridProvider
	}

	if err != nil {
		return nil, err
	}

	return client, nil
}

// FilterDebrid returns clients that match the filter function
func (m *Manager) FilterDebrid(filter func(debrid.Client) bool) []debrid.Client {
	var filtered []debrid.Client

	m.clients.Range(func(key string, client debrid.Client) bool {
		if client != nil && filter(client) {
			filtered = append(filtered, client)
		}
		return true
	})
	return filtered
}

// syncAccountsWorker periodically syncs account information from all debrid services
func (m *Manager) syncAccountsWorker(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Initial sync
	_ = m.syncAccounts()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Info().Msg("Account sync worker stopped")
			return
		case <-ticker.C:
			_ = m.syncAccounts()
		}
	}
}

// syncAccounts syncs account information from all debrid services
func (m *Manager) syncAccounts() error {

	m.clients.Range(func(name string, client debrid.Client) bool {
		if client == nil {
			return true
		}

		_log := client.Logger()
		if err := client.SyncAccounts(); err != nil {
			_log.Error().Err(err).Str("debrid", name).Msg("Failed to sync account")
			return true
		}
		_log.Info().Str("debrid", name).Msg("Account synced successfully")
		return true
	})
	return nil
}

func (m *Manager) GetIngests() ([]types.IngestData, error) {
	torrents, err := m.GetTorrents(nil)
	if err != nil {
		return nil, err
	}
	var ingests []types.IngestData
	for _, torrent := range torrents {
		ingests = append(ingests, types.IngestData{
			Debrid: torrent.ActiveDebrid,
			Name:   torrent.OriginalFilename,
			Hash:   torrent.InfoHash,
			Size:   torrent.Bytes,
		})
	}
	return ingests, nil
}

func (m *Manager) GetIngestsByDebrid(debridName string) ([]types.IngestData, error) {
	torrents, err := m.GetTorrents(nil)
	if err != nil {
		return nil, err
	}
	var ingests []types.IngestData
	for _, torrent := range torrents {
		if !torrent.HasPlacement(debridName) {
			continue
		}
		ingests = append(ingests, types.IngestData{
			Debrid: torrent.ActiveDebrid,
			Name:   torrent.OriginalFilename,
			Hash:   torrent.InfoHash,
			Size:   torrent.Bytes,
		})
	}
	return ingests, nil
}
