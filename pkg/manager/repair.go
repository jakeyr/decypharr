package manager

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sourcegraph/conc/pool"
)

type RepairManager interface {
	Run(ctx context.Context)
	AddJob(arrsNames []string, mediaIDs []string, autoProcess, recurrent bool) error
	StopJob(id string) error
	ProcessJob(id string) error
	DeleteJobs(ids []string)
	GetJobs() []*storage.Job
	Stop()
}

func (m *Manager) GetBrokenFiles(item *storage.EntryItem, filenames []string) []string {
	if len(item.Files) == 0 {
		return filenames
	}

	cfg := config.Get()

	repairStrategy := cfg.Repair.Strategy

	// Select which files to check
	files := make(map[string]*storage.File)
	if len(filenames) > 0 {
		for _, name := range filenames {
			if f, ok := item.Files[name]; ok {
				files[name] = f
			}
		}
	} else {
		files = item.Files
	}

	entries := make(map[string]*storage.Entry)
	badFiles := xsync.NewMap[string, []*storage.File]()

	// First pass: load entries by infohash
	for _, file := range files {
		if _, ok := entries[file.InfoHash]; !ok {
			entry, err := m.storage.Get(file.InfoHash)
			if err != nil {
				m.logger.Error().Err(err).
					Str("infohash", file.InfoHash).
					Msg("Failed to get entry from storage")
				continue
			}
			entries[file.InfoHash] = entry
		}
	}

	// Second pass: check links in parallel using conc pool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	torrentWideFailed := atomic.Bool{}

	handleBroken := func(file *storage.File) {
		if repairStrategy == config.RepairStrategyPerTorrent {
			torrentWideFailed.Store(true)
			cancel() // stop other goroutines early
		} else {
			badFiles.Compute(file.InfoHash, func(oldValue []*storage.File, loaded bool) ([]*storage.File, xsync.ComputeOp) {
				if !loaded {
					return []*storage.File{file}, xsync.UpdateOp
				}
				return append(oldValue, file), xsync.UpdateOp
			})
		}
	}

	// Limit concurrency across all files
	p := pool.New().
		WithContext(ctx)

	for name, file := range files {
		p.Go(func(ctx context.Context) error {
			// Respect cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			entry, ok := entries[file.InfoHash]
			if !ok {
				return nil
			}

			if entry.IsNZB() && cfg.Usenet.SkipRepair {
				// NZB repair disabled, skip checking
				return nil
			}

			// If entry is NZB use Usenet client to check link validity
			if entry.IsNZB() {
				if m.usenet == nil {
					m.logger.Error().
						Str("infohash", entry.InfoHash).
						Msg("Usenet client not configured, cannot check NZB links")
					return nil
				}
				if err := m.usenet.CheckFile(ctx, entry.InfoHash, file.Name); err != nil {
					if errors.Is(err, customerror.UsenetSegmentMissingError) {
						handleBroken(file)
					}
				}
				return nil
			}

			client := m.ProviderClient(entry.ActiveProvider)
			if client == nil {
				m.logger.Error().
					Str("debrid", entry.ActiveProvider).
					Msg("Provider client not found")
				return nil
			}

			placement := entry.GetActiveProvider()
			placementFile := placement.Files[name]

			// Missing placement or link → broken
			if placementFile == nil || (placementFile.Link == "" && placementFile.Id == "") {
				handleBroken(file)
				return nil
			}

			link := placementFile.Link
			if link == "" {
				link = placementFile.Id
			}
			if link == "" {
				handleBroken(file)
				return nil
			}

			// Check if link is still valid
			if err := client.CheckFile(ctx, file.InfoHash, link); err != nil {
				if errors.Is(err, customerror.HosterUnavailableError) {
					handleBroken(file)
				}
			}

			return nil
		})
	}

	// We don't really care about the pool error here (ctx.err etc.)
	_ = p.Wait()

	// Strategy: per_torrent ⇒ any failure means all files are broken
	if repairStrategy == config.RepairStrategyPerTorrent && torrentWideFailed.Load() {
		for _, file := range files {
			badFiles.Compute(file.InfoHash, func(oldValue []*storage.File, loaded bool) ([]*storage.File, xsync.ComputeOp) {
				if !loaded {
					return []*storage.File{file}, xsync.UpdateOp
				}
				return append(oldValue, file), xsync.UpdateOp
			})
		}
	}
	// Time to attempt repair of bad files
	brokenFiles := make([]string, 0)
	badFiles.Range(func(infohash string, files []*storage.File) bool {
		entry, err := m.storage.Get(infohash)
		if err != nil {
			for _, file := range files {
				brokenFiles = append(brokenFiles, file.Name)
			}
			return true
		}
		if entry.IsNZB() {
			// We can't repair NZB files here
			for _, file := range files {
				brokenFiles = append(brokenFiles, file.Name)
			}
			return true
		}
		// Attempt to re-insert the torrent
		if err = m.ReinsertEntry(context.Background(), entry); err != nil {
			for _, file := range files {
				brokenFiles = append(brokenFiles, file.Name)
			}
			return true
		}
		return true
	})
	mappedBadFiles := make(map[string]bool)
	for _, name := range brokenFiles {
		if _, ok := mappedBadFiles[name]; !ok {
			mappedBadFiles[name] = true
		}
	}
	result := make([]string, 0, len(mappedBadFiles))
	for name := range mappedBadFiles {
		result = append(result, name)
	}
	return result
}

func (m *Manager) ReinsertEntry(ctx context.Context, entry *storage.Entry) error {
	if m.fixer == nil {
		return fmt.Errorf("fixer not initialized")
	}
	res, err := m.fixer.FixTorrent(ctx, entry, false)
	if err != nil {
		return err
	}
	if !res.Success {
		return fmt.Errorf("failed to re-insert torrent")
	}
	return nil
}
