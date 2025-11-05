package manager

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"golang.org/x/sync/singleflight"
)

// Manager handles unified torrent management - replaces wire.Store completely
type Manager struct {
	storage    *storage.Storage
	migrator   *Migrator
	clients    *xsync.Map[string, debrid.Client]
	arr        *arr.Storage
	logger     zerolog.Logger
	mu         sync.RWMutex
	ready      chan struct{}
	readyOnce  sync.Once
	mountPaths map[string]*FileInfo

	// Migration jobs tracking
	migrationJobs   *xsync.Map[string, *storage.SwitcherJob]
	refreshInterval time.Duration

	config config.Manager

	// Processing workers
	downloadSem  chan struct{}
	skipPreCache bool
	scheduler    gocron.Scheduler
	cetScheduler gocron.Scheduler
	queue        *Queue

	// downloading
	downloadSG           singleflight.Group
	refreshSG            singleflight.Group
	invalidDownloadLinks *xsync.Map[string, string]
	failedLinksCounter   *xsync.Map[string, atomic.Int32]

	// Multi-debrid error tracking for automatic switching
	// Key format: "infohash:operation" where operation is "download" or "stream"
	torrentErrorCounters *xsync.Map[string, atomic.Int32]

	// repair
	fixer *Fixer
	ctx   context.Context

	customFolders *CustomFolders
	streamClient  *http.Client
	mountManager  MountManager

	startTime time.Time

	event *Event
}

var (
	instance *Manager
	once     sync.Once
)

// New creates a new Manager instance
func New() *Manager {
	cfg := config.Get()
	_logger := logger.New("torrent-manager")

	// Create storage directory
	dbPath := filepath.Join(cfg.Path, "decypharr.db")

	strg, err := storage.NewStorage(dbPath)
	if err != nil {
		panic(fmt.Errorf("failed to create manager storage: %w", err))
	}

	// Initialize debrid registry

	ctx := context.Background()

	// Optimized transport for high-performance streaming
	// DNS resolver with caching
	dialer := &net.Dialer{
		Timeout:   10 * time.Second, // Fast connection timeout
		KeepAlive: 30 * time.Second, // Keep connections alive
	}

	transport := &http.Transport{
		// TLS Configuration
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
			// Session resumption for faster TLS handshakes
			ClientSessionCache: tls.NewLRUClientSessionCache(100),
		},
		TLSHandshakeTimeout: 10 * time.Second, // Faster than 30s

		// Connection Pooling (aggressive for streaming)
		MaxIdleConns:        200, // Increased from 100 (support more concurrent streams)
		MaxIdleConnsPerHost: 50,  // Increased from 20 (multiple CDN hosts per debrid)
		MaxConnsPerHost:     100, // Limit total connections per host
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,

		// HTTP/2 Support (much faster for multiple requests)
		ForceAttemptHTTP2: true,

		// Timeouts
		ResponseHeaderTimeout: 30 * time.Second, // Faster than 60s
		ExpectContinueTimeout: 1 * time.Second,  // For large uploads
		DisableCompression:    true,             // Already streaming compressed video

		// Connection settings
		DialContext: dialer.DialContext,
		// Custom DialTLSContext for connection tracking (optional)
		// Proxy support from environment
		Proxy: http.ProxyFromEnvironment,

		// Read/Write buffer sizes (optimized for streaming)
		WriteBufferSize: 64 * 1024,  // 64KB write buffer
		ReadBufferSize:  256 * 1024, // 256KB read buffer (faster downloads)
	}

	streamClient := &http.Client{
		Transport: transport,
		Timeout:   0,
	}

	instance = &Manager{
		storage:              strg,
		clients:              xsync.NewMap[string, debrid.Client](),
		logger:               _logger,
		migrationJobs:        xsync.NewMap[string, *storage.SwitcherJob](),
		downloadSem:          make(chan struct{}, cfg.Manager.MaxDownloads),
		config:               cfg.Manager,
		arr:                  arr.NewStorage(),
		queue:                newQueue(ctx, strg, 1000, cfg.RemoveStalledAfter),
		mountPaths:           make(map[string]*FileInfo),
		invalidDownloadLinks: xsync.NewMap[string, string](),
		failedLinksCounter:   xsync.NewMap[string, atomic.Int32](),
		torrentErrorCounters: xsync.NewMap[string, atomic.Int32](),
		ctx:                  ctx,
		streamClient:         streamClient,
		ready:                make(chan struct{}),
	}

	instance.init()

	// Create migrator
	return instance
}

func (m *Manager) init() {
	cfg := config.Get()
	scheduler, err := gocron.NewScheduler(gocron.WithLocation(time.Local), gocron.WithGlobalJobOptions(gocron.WithTags("decypharr-manager")))
	if err != nil {
		scheduler, _ = gocron.NewScheduler(gocron.WithGlobalJobOptions(gocron.WithTags("decypharr-manager")))
	}

	// Create CET scheduler for time-specific jobs
	cetLocation, err := time.LoadLocation("CET")
	if err != nil {
		cetLocation = time.UTC
	}
	cetScheduler, err := gocron.NewScheduler(gocron.WithLocation(cetLocation), gocron.WithGlobalJobOptions(gocron.WithTags("decypharr-cet")))
	if err != nil {
		cetScheduler, _ = gocron.NewScheduler(gocron.WithGlobalJobOptions(gocron.WithTags("decypharr-cet")))
	}

	m.config = cfg.Manager
	m.downloadSem = make(chan struct{}, cfg.Manager.MaxDownloads)

	// Recreate queue with new config
	m.queue = newQueue(m.ctx, m.storage, 1000, cfg.RemoveStalledAfter)

	// Clear debrid clients so they get recreated with new config
	m.clients = xsync.NewMap[string, debrid.Client]()

	// Reset ready channel and sync.Once for the next start
	m.ready = make(chan struct{})
	m.readyOnce = sync.Once{}

	instance.scheduler = scheduler
	instance.cetScheduler = cetScheduler
	instance.migrator = NewMigrator(m.storage)

	refreshInterval, err := time.ParseDuration(cfg.Manager.RefreshInterval)
	if err != nil {
		refreshInterval = 15 * time.Minute
	}
	instance.refreshInterval = refreshInterval

	// initialize debrid clients
	instance.initDebridClients()

	// Init custom folders

	instance.initCustomFolders()

	// Initialize fixer
	instance.fixer = NewFixer(instance)

	// Set mount paths
	instance.setMountPaths()
}

func (m *Manager) migrate() {
	m.logger.Info().Msg("Checking for cache files to migrate...")

	// Check if migration has already been done
	status, err := m.migrator.GetStatus()
	if err == nil && !status.Running && status.Completed > 0 {
		m.logger.Info().
			Int("completed", status.Completed).
			Int("errors", status.Errors).
			Msg("Migration already completed previously")
		return
	}

	// Get migration stats to see if there are cache files
	stats, err := m.migrator.GetStats()
	if err != nil {
		m.logger.Warn().Err(err).Msg("Failed to get migration stats")
		return
	}

	cacheFiles, ok := stats["cache_files"].(int)
	if !ok || cacheFiles == 0 {
		m.logger.Info().Msg("No cache files found to migrate")
		return
	}

	cacheTorrents, ok := stats["cache_torrents"].(int)
	if !ok {
		cacheTorrents = 0
	}

	m.logger.Info().
		Int("cache_files", cacheFiles).
		Int("unique_torrents", cacheTorrents).
		Msg("Found cache files, starting automatic migration...")

	// Start migration with backup
	if err := m.migrator.Start(); err != nil {
		m.logger.Error().Err(err).Msg("Failed to start automatic migration")
		return
	}

	m.logger.Info().Msg("Automatic migration started successfully")
}

func (m *Manager) sync(ctx context.Context) error {
	// First time sync debrid -> storage
	m.logger.Info().
		Int("debrids", m.clients.Size()).
		Msg("Performing initial sync of torrents from debrid clients...")
	var wg sync.WaitGroup
	m.clients.Range(func(name string, client debrid.Client) bool {
		wg.Add(1)
		defer wg.Done()
		m.refreshTorrents(ctx, name, client)
		return true
	})
	wg.Wait()
	m.logger.Info().
		Msg("Initial sync of torrents from debrid clients completed")
	return nil
}

func (m *Manager) SetMountManager(mountMgr MountManager) {
	m.mountManager = mountMgr
}

// Start starts the manager and all its components
func (m *Manager) Start(ctx context.Context) error {
	m.startTime = time.Now()
	m.logger.Info().Msg("Starting manager")

	// run the migration process
	m.migrate()

	// Then perform initial sync
	if err := m.sync(ctx); err != nil {
		return fmt.Errorf("failed to perform initial sync: %w", err)
	}

	// Start workers
	if err := m.StartWorker(ctx); err != nil {
		return fmt.Errorf("failed to start manager worker: %w", err)
	}

	// Close ready channel once, safe for multiple calls
	m.readyOnce.Do(func() {
		close(m.ready)
	})

	// Start the mount manager if set
	// This also start thr mounting process
	if m.mountManager != nil {
		if err := m.mountManager.Start(ctx); err != nil {
			return fmt.Errorf("failed to start mount manager: %w", err)
		}
	}

	return nil
}

// Reset resets the manager with the new configuration
// This is called after config changes (e.g., setup wizard) to apply new settings
func (m *Manager) Reset() error {
	m.logger.Info().Msg("Resetting manager with new configuration")

	// Stop schedulers
	if m.scheduler != nil {
		if err := m.scheduler.Shutdown(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to shutdown scheduler")
		}
	}
	if m.cetScheduler != nil {
		if err := m.cetScheduler.Shutdown(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to shutdown CET scheduler")
		}
	}

	// Reload configuration
	m.init()
	m.logger.Info().Msg("Manager reset complete")
	return nil
}

func (m *Manager) GetStats() (map[string]interface{}, error) {
	count, err := m.storage.Count()
	if err != nil {
		return nil, err
	}

	storageStats := m.storage.Stats()
	activeJobs := 0
	completedJobs := 0
	failedJobs := 0
	m.migrationJobs.Range(func(_ string, job *storage.SwitcherJob) bool {
		switch job.Status {
		case "pending", "submitting", "downloading":
			activeJobs++
		case "completed":
			completedJobs++
		case "failed", "cancelled":
			failedJobs++
		}
		return true
	})

	return map[string]interface{}{
		"total_torrents": count,
		"storage_stats":  storageStats,
		"active_jobs":    activeJobs,
		"completed_jobs": completedJobs,
		"failed_jobs":    failedJobs,
	}, nil
}

func (m *Manager) IsReady() chan struct{} {
	return m.ready
}

func (m *Manager) Uptime() time.Duration {
	return time.Since(m.startTime)
}

func (m *Manager) StartTime() time.Time {
	return m.startTime
}

// CRUD operations

// GetTorrent gets a torrent by name
func (m *Manager) GetTorrent(infohash string) (*storage.Torrent, error) {
	return m.storage.Get(infohash)
}

func (m *Manager) GetTorrentByHashAndCategory(infohash, category string) (*storage.Torrent, error) {
	return m.storage.GetByHashAndCategory(infohash, category)
}

// GetTorrentByName gets a torrent by name
func (m *Manager) GetTorrentByName(name string) (*storage.Torrent, error) {
	return m.storage.GetByName(name)
}

func (m *Manager) RemoveTorrentFile(torrentName, filename string) error {
	m.logger.Debug().Str("torrent", torrentName).Msgf("Removing file %s", filename)
	torr, err := m.GetTorrentByName(torrentName)
	if err != nil {
		return fmt.Errorf("torrent %s not found", torrentName)
	}
	file, err := torr.GetFile(filename)
	if err != nil {
		return fmt.Errorf("file %s not found in torrent %s", filename, torrentName)
	}
	file.Deleted = true
	torr.Files[filename] = file

	// If the torrent has no files left, delete it
	if len(torr.GetActiveFiles()) == 0 {
		m.logger.Debug().Msgf("Torrent %s has no files left, deleting it", torr.InfoHash)
		if err := m.DeleteTorrent(torr.InfoHash); err != nil {
			return fmt.Errorf("failed to delete torrent %s: %w", torr.InfoHash, err)
		}
		return nil
	}
	return m.UpdateTorrent(torr)
}

func (m *Manager) GetTorrents(filter func(*storage.Torrent) bool) ([]*storage.Torrent, error) {
	return m.storage.List(filter)
}

func (m *Manager) GetTorrentsCount() (int, error) {
	return m.storage.Count()
}

func (m *Manager) UpdateTorrent(torrent *storage.Torrent) error {
	torrent.UpdatedAt = time.Now()
	if err := m.storage.Update(torrent); err != nil {
		return err
	}
	return nil
}

// DeleteTorrent deletes a torrent by infohash
func (m *Manager) DeleteTorrent(infohash string) error {
	torr, err := m.GetTorrent(infohash)
	if err != nil {
		return err
	}
	// Delete active placements from debrid clients

	go func() {
		if err := m.RemoveActivePlacements(torr); err != nil {
			m.logger.Error().Err(err).Msgf("Failed to remove active placements for torrent %s", infohash)
		}
	}()

	return m.storage.Delete(infohash)
}

func (m *Manager) DeleteTorrents(infohashes []string) error {
	for _, infohash := range infohashes {
		if err := m.DeleteTorrent(infohash); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) GetMigrationJob(jobID string) (*storage.SwitcherJob, error) {
	job, exists := m.migrationJobs.Load(jobID)
	if !exists {
		return nil, fmt.Errorf("migration job not found: %s", jobID)
	}
	return job, nil
}

// QBit API Compatibility Methods

// === Queue ===

func (m *Manager) trackAvailableSlots(ctx context.Context) {
	// This function tracks the available slots for each debrid client
	availableSlots := make(map[string]int)

	m.clients.Range(func(name string, client debrid.Client) bool {
		slots, err := client.GetAvailableSlots()
		if err != nil {
			return true
		}
		availableSlots[name] = slots
		return true
	})

	if len(availableSlots) == 0 {
		m.logger.Debug().Msg("No debrid clients available or no slots found")
		return // No debrid clients or slots available, nothing to process
	}

	if m.queue.RequestsSize() <= 0 {
		// Queue is empty, no need to process
		return
	}

	for name, slots := range availableSlots {
		m.logger.Debug().Msgf("Available slots for %s: %d", name, slots)
		// If slots are available, process the next import request from the queue
		for slots > 0 {
			select {
			case <-ctx.Done():
				return // Exit if context is done
			default:
				if err := m.processFromQueue(ctx); err != nil {
					m.logger.Error().Err(err).Msg("Error processing from queue")
					return // Exit on error
				}
				slots-- // Decrease the available slots after processing
			}
		}
	}
}

func (m *Manager) processFromQueue(ctx context.Context) error {
	// Pop the next import request from the queue
	importReq, err := m.queue.PopRequest()
	if err != nil {
		return err
	}
	if importReq == nil {
		return nil
	}
	return m.AddTorrent(ctx, importReq)
}
