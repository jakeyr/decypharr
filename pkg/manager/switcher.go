package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	torrentpkg "github.com/sirrobot01/decypharr/pkg/storage"
)

func ptrTime(t time.Time) *time.Time {
	return &t
}

// This is in-charge of moving torrents between different debrid services

// SwitchTorrent moves a torrent from one debrid to another
func (m *Manager) SwitchTorrent(ctx context.Context, infohash, targetDebrid string, keepOld, waitComplete bool) (*torrentpkg.SwitcherJob, error) {
	// Get the torrent
	torrent, err := m.GetTorrent(infohash)
	if err != nil {
		return nil, fmt.Errorf("failed to get torrent: %w", err)
	}

	// Check if already on target debrid
	if torrent.ActiveDebrid == targetDebrid {
		return nil, torrentpkg.ErrAlreadyOnDebrid
	}

	// Need to actually migrate - create job
	job := &torrentpkg.SwitcherJob{
		ID:           uuid.New().String(),
		InfoHash:     infohash,
		SourceDebrid: torrent.ActiveDebrid,
		TargetDebrid: targetDebrid,
		Status:       "pending",
		Progress:     0,
		CreatedAt:    time.Now(),
		KeepOld:      keepOld,
		WaitComplete: waitComplete,
	}

	// Store job
	m.migrationJobs.Store(job.ID, job)

	// Start migration in background
	go m.executeMigration(ctx, job, torrent)

	return job, nil
}

// executeMigration performs the actual torrent migration - COMPLETE IMPLEMENTATION
func (m *Manager) executeMigration(ctx context.Context, job *torrentpkg.SwitcherJob, torrent *torrentpkg.Torrent) {
	m.logger.Info().
		Str("job_id", job.ID).
		Str("torrent", torrent.Name).
		Str("source", job.SourceDebrid).
		Str("target", job.TargetDebrid).
		Msg("Starting torrent migration")

	// Update torrent status
	torrent.Status = debridTypes.TorrentStatusSwitching
	_ = m.UpdateTorrent(torrent)

	// Get target debrid client
	targetClient := m.DebridClient(job.TargetDebrid)
	if targetClient == nil {
		job.Status = "failed"
		job.Error = fmt.Sprintf("target debrid %s not found", job.TargetDebrid)
		job.CompletedAt = ptrTime(time.Now())
		return
	}
	// Submit to target debrid
	job.Status = "submitting"
	job.Progress = 10

	success, err := m.fixer.MoveTorrent(torrent, job.TargetDebrid, false) // false = don't force re-download

	if err != nil || !success {
		job.Status = "failed"
		job.Error = fmt.Sprintf("failed to move torrent to target debrid: %v", err)
		job.CompletedAt = ptrTime(time.Now())
		m.logger.Error().
			Err(err).
			Str("job_id", job.ID).
			Msg("Failed to move torrent to target debrid")
		return
	}

	// Handle source placement
	// This removes the old placement
	if !job.KeepOld {
		// Archive and optionally delete from source
		sourcePlacement, ok := torrent.Placements[job.SourceDebrid]
		if ok && sourcePlacement != nil {
			torrent.RemovePlacement(job.SourceDebrid, func(placement *torrentpkg.Placement) error {
				return m.RemoveFromDebrid(sourcePlacement)
			})
		}
	}

	// Save updated torrent
	if err := m.UpdateTorrent(torrent); err != nil {
		job.Status = "failed"
		job.Error = fmt.Sprintf("failed to update torrent: %v", err)
		m.logger.Error().Err(err).Msg("Failed to update torrent after migration")
	} else {
		job.Status = "completed"
		job.Progress = 100
	}

	job.CompletedAt = ptrTime(time.Now())

	m.logger.Info().
		Str("job_id", job.ID).
		Str("status", string(job.Status)).
		Msg("Migration completed")
}
