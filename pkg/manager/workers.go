package manager

import (
	"context"

	"github.com/go-co-op/gocron/v2"
	"github.com/sirrobot01/decypharr/internal/utils"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
)

func (m *Manager) StartWorkers(ctx context.Context) error {
	go m.syncAccountsWorker(ctx)
	return nil
}

func (m *Manager) addQueueProcessorJob(ctx context.Context) error {
	// This function is responsible for starting queue processing scheduled tasks

	if jd, err := utils.ConvertToJobDef("30s"); err != nil {
		m.logger.Error().Err(err).Msg("Failed to convert slots tracking interval to job definition")
	} else {
		// Schedule the job
		if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
			m.trackAvailableSlots(ctx)
		}), gocron.WithContext(ctx)); err != nil {
			m.logger.Error().Err(err).Msg("Failed to create slots tracking job")
		} else {
			m.logger.Trace().Msgf("Slots tracking job scheduled for every %s", "30s")
		}
	}

	if m.config.RemoveStalledAfter != "" {
		// Stalled torrents removal job
		if jd, err := utils.ConvertToJobDef("1m"); err != nil {
			m.logger.Error().Err(err).Msg("Failed to convert remove stalled torrents interval to job definition")
		} else {
			// Schedule the job
			if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
				err := m.queue.DeleteStalled()
				if err != nil {
					m.logger.Error().Err(err).Msg("Failed to process remove stalled torrents")
				}
			}), gocron.WithContext(ctx)); err != nil {
				m.logger.Error().Err(err).Msg("Failed to create remove stalled torrents job")
			} else {
				m.logger.Trace().Msgf("Remove stalled torrents job scheduled for every %s", "1m")
			}
		}
	}
	return nil
}

func (m *Manager) StartWorker(ctx context.Context) error {

	// Stop any existing jobs before starting new ones
	m.scheduler.RemoveByTags("decypharr")

	if err := m.addQueueProcessorJob(ctx); err != nil {
		return err
	}

	// Schedule per-debrid refresh jobs
	m.clients.Range(func(debridName string, debridClient debrid.Client) bool {
		if debridClient == nil {
			return true
		}

		debridConfig := debridClient.Config()

		// Schedule download link refresh job for this debrid
		if jd, err := utils.ConvertToJobDef(debridConfig.DownloadLinksRefreshInterval); err != nil {
			m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to convert download link refresh interval to job definition")
		} else {
			jobName := debridName + "-download-links"
			if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
				m.refreshDownloadLinks(ctx, debridName, debridClient)
			}), gocron.WithContext(ctx), gocron.WithName(jobName)); err != nil {
				m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to create download link refresh job")
			} else {
				m.logger.Debug().Str("debrid", debridName).Msgf("Download link refresh job scheduled for every %s", debridConfig.DownloadLinksRefreshInterval)
			}
		}

		// Schedule torrent refresh job for this debrid
		if jd, err := utils.ConvertToJobDef(debridConfig.TorrentsRefreshInterval); err != nil {
			m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to convert torrent refresh interval to job definition")
		} else {
			jobName := debridName + "-torrents"
			if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
				m.refreshTorrents(ctx, debridName, debridClient)
			}), gocron.WithContext(ctx), gocron.WithName(jobName)); err != nil {
				m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to create torrent refresh job")
			} else {
				m.logger.Debug().Str("debrid", debridName).Msgf("Torrent refresh job scheduled for every %s", debridConfig.TorrentsRefreshInterval)
			}
		}

		return true
	})

	// Schedule the reset invalid links job
	// This job will run every at 00:00 CET
	// and reset the invalid links in the cache
	if jd, err := utils.ConvertToJobDef("00:00"); err != nil {
		m.logger.Error().Err(err).Msg("Failed to convert link reset interval to job definition")
	} else {
		// Schedule the job
		if _, err := m.cetScheduler.NewJob(jd, gocron.NewTask(func() {
			// Reset invalid download links map at midnight CET
			if m.invalidDownloadLinks != nil {
				m.invalidDownloadLinks.Clear()
				m.logger.Debug().Msg("Cleared invalid download links cache")
			}
			// Reset failed links counter
			m.failedLinksCounter.Clear()
			m.logger.Debug().Msg("Cleared failed links counter")
		}), gocron.WithContext(ctx)); err != nil {
			m.logger.Error().Err(err).Msg("Failed to create link reset job")
		} else {
			m.logger.Debug().Msgf("Link reset job scheduled for every midnight, CET")
		}
	}

	// Start the scheduler
	m.scheduler.Start()
	m.cetScheduler.Start()
	return nil
}
