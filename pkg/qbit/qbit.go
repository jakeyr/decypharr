package qbit

import (
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

type QBit struct {
	DownloadFolder      string
	Categories          []string
	AlwaysRmTrackerUrls bool
	logger              zerolog.Logger
	Tags                []string
	manager             *manager.Manager
}

func New(manager *manager.Manager) *QBit {
	_cfg := config.Get()
	cfg := _cfg.Manager
	return &QBit{
		DownloadFolder:      cfg.DownloadFolder,
		Categories:          cfg.Categories,
		AlwaysRmTrackerUrls: cfg.AlwaysRmTrackerUrls,
		manager:             manager,
		logger:              logger.New("qbit"),
	}
}

func (q *QBit) Reset() {
	// Manager is a singleton, no reset needed
	q.Tags = nil
}
