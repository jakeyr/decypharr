package repair

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sourcegraph/conc/pool"
	"golang.org/x/sync/errgroup"
)

type contexts struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type Repair struct {
	manager     *manager.Manager
	autoProcess bool
	logger      zerolog.Logger
	workers     int
	scheduler   gocron.Scheduler

	debridPathCache sync.Map // debridPath:debridName cache.Emptied after each run
	ctx             context.Context

	activeContexts *xsync.Map[string, contexts] // jobID:context
}

func New(mgr *manager.Manager) *Repair {
	cfg := config.Get()
	workers := runtime.NumCPU() * 20
	if cfg.Repair.Workers > 0 {
		workers = cfg.Repair.Workers
	}

	r := &Repair{
		logger:         logger.New("repair"),
		autoProcess:    cfg.Repair.AutoProcess,
		manager:        mgr,
		workers:        workers,
		ctx:            context.Background(),
		activeContexts: xsync.NewMap[string, contexts](),
	}

	return r
}

func (r *Repair) Run(ctx context.Context) {
	if err := r.addJobWithContext(ctx, []string{}, []string{}, r.autoProcess, true); err != nil {
		r.logger.Error().Err(err).Msg("Error running repair job")
	}
}

func (r *Repair) Stop() {
	r.activeContexts.Range(func(key string, value contexts) bool {
		value.cancel()
		return true
	})
}

func (r *Repair) getArrs(arrNames []string) []string {
	arrs := make([]string, 0)
	if len(arrNames) == 0 {
		// No specific arrs, get all
		// Also check if any arrs are set to skip repair
		_arrs := r.manager.Arr().GetAll()
		for _, a := range _arrs {
			if a.SkipRepair {
				continue
			}
			arrs = append(arrs, a.Name)
		}
	} else {
		for _, name := range arrNames {
			a := r.manager.Arr().Get(name)
			if a == nil || a.Host == "" || a.Token == "" {
				continue
			}
			arrs = append(arrs, a.Name)
		}
	}

	return arrs
}

func jobKey(arrNames []string, mediaIDs []string) string {
	return fmt.Sprintf("%s-%s", strings.Join(arrNames, ","), strings.Join(mediaIDs, ","))
}

func (r *Repair) reset(j *storage.Job) {
	// Update job for rerun
	j.Status = storage.JobStarted
	j.StartedAt = time.Now()
	j.CompletedAt = time.Time{}
	j.FailedAt = time.Time{}
	j.BrokenItems = nil
	j.Error = ""
	if j.Recurrent || j.Arrs == nil {
		j.Arrs = r.getArrs([]string{}) // GetReader new arrs
	}
}

func (r *Repair) newJob(arrsNames []string, mediaIDs []string) *storage.Job {
	arrs := r.getArrs(arrsNames)
	return &storage.Job{
		ID:        uuid.New().String(),
		Arrs:      arrs,
		MediaIDs:  mediaIDs,
		StartedAt: time.Now(),
		Status:    storage.JobStarted,
	}
}

// initRun initializes the repair run, setting up necessary configurations, checks and caches
func (r *Repair) initRun(ctx context.Context) {
}

// // onComplete is called when the repair job is completed
func (r *Repair) onComplete() {
	// Set the cache maps to nil
	r.debridPathCache = sync.Map{}
}

func (r *Repair) preRunChecks() error {
	if r.manager == nil {
		return fmt.Errorf("manager not initialized")
	}
	return nil
}

func (r *Repair) saveToStorage(job *storage.Job) {
	if job == nil {
		return
	}
	if err := r.manager.Storage().SaveRepairJob(jobKey(job.Arrs, job.MediaIDs), job); err != nil {
		r.logger.Error().Err(err).Msgf("Failed to save job %s to storage", job.ID)
	}
}

func (r *Repair) repair(ctx context.Context, job *storage.Job) error {
	defer r.saveToStorage(job)
	if err := r.preRunChecks(); err != nil {
		return err
	}

	// Initialize the run
	r.initRun(ctx)

	// Determine which repair mode to use
	var err error
	var brokenItems map[string][]arr.ContentFile

	brokenItems, err = r.repairArrMode(ctx, job)

	if err != nil {
		// Check if job was canceled
		if errors.Is(ctx.Err(), context.Canceled) {
			job.Status = storage.JobCancelled
			job.CompletedAt = time.Now()
			job.Error = "Job was cancelled"
			return fmt.Errorf("job cancelled")
		}

		job.FailedAt = time.Now()
		job.Error = err.Error()
		job.Status = storage.JobFailed
		job.CompletedAt = time.Now()
		r.manager.Notifications.Notify(notifications.Event{
			Type:    config.EventRepairFailed,
			Status:  "error",
			Message: job.DiscordContext(),
			Error:   err,
		})
		return err
	}

	if len(brokenItems) == 0 {
		job.CompletedAt = time.Now()
		job.Status = storage.JobCompleted

		r.manager.Notifications.Notify(notifications.Event{
			Type:    config.EventRepairComplete,
			Status:  "success",
			Message: job.DiscordContext(),
		})

		return nil
	}

	job.BrokenItems = brokenItems
	if job.AutoProcess {
		// Job is already processed
		job.CompletedAt = time.Now() // Mark as completed
		job.Status = storage.JobCompleted
		r.manager.Notifications.Notify(notifications.Event{
			Type:    config.EventRepairComplete,
			Status:  "success",
			Message: job.DiscordContext(),
		})
	} else {
		job.Status = storage.JobPending
		r.manager.Notifications.Notify(notifications.Event{
			Type:    config.EventRepairPending,
			Status:  "pending",
			Message: job.DiscordContext(),
		})
	}
	return nil
}

// repairArrMode repairs based on Arr services (the original repair logic)
func (r *Repair) repairArrMode(ctx context.Context, job *storage.Job) (map[string][]arr.ContentFile, error) {
	// Use a mutex to protect concurrent access to brokenItems
	var mu sync.Mutex
	brokenItems := map[string][]arr.ContentFile{}
	g, ctx := errgroup.WithContext(ctx)

	for _, a := range job.Arrs {
		g.Go(func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			var items []arr.ContentFile
			var err error

			if len(job.MediaIDs) == 0 {
				items, err = r.repairArr(ctx, job, a, "")
				if err != nil {
					r.logger.Error().Err(err).Msgf("Error repairing %s", a)
					return err
				}
			} else {
				for _, id := range job.MediaIDs {
					someItems, err := r.repairArr(ctx, job, a, id)
					if err != nil {
						r.logger.Error().Err(err).Msgf("Error repairing %s with ID %s", a, id)
						return err
					}
					items = append(items, someItems...)
				}
			}

			// Safely append the found items to the shared slice
			if len(items) > 0 {
				mu.Lock()
				brokenItems[a] = items
				mu.Unlock()
			}

			return nil
		})
	}

	// Wait for all goroutines to complete and check for errors
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return brokenItems, nil
}

func (r *Repair) repairArr(ctx context.Context, job *storage.Job, _arr string, tmdbId string) ([]arr.ContentFile, error) {
	brokenItems := make([]arr.ContentFile, 0)
	a := r.manager.Arr().Get(_arr)
	if a == nil {
		return brokenItems, fmt.Errorf("arr %s not found", _arr)
	}
	if tmdbId == "" {
		r.logger.Info().Msgf("Starting repair for all %s media", a.Name)
	} else {
		r.logger.Info().Msgf("Starting repair for %s media with TMDB ID %s", a.Name, tmdbId)
	}
	media, err := a.GetMedia(tmdbId)
	if err != nil {
		r.logger.Info().Msgf("Failed to get %s media: %v", a.Name, err)
		return brokenItems, err
	}
	r.logger.Info().Msgf("Found %d %s media", len(media), a.Name)

	if len(media) == 0 {
		r.logger.Info().Msgf("No %s media found", a.Name)
		return brokenItems, nil
	}

	// Mutex for brokenItems
	var mu sync.Mutex
	var wg sync.WaitGroup
	workerChan := make(chan arr.Content, min(len(media), r.workers))

	for i := 0; i < r.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for m := range workerChan {
				select {
				case <-ctx.Done():
					return
				default:
				}
				items := r.getBrokenFiles(ctx, m)
				if items != nil {
					r.logger.Debug().Msgf("Found %d broken files for %s", len(items), m.Title)
					if job.AutoProcess {
						r.logger.Info().Msgf("Auto processing %d broken items for %s", len(items), m.Title)

						// Delete broken items
						if err := a.DeleteFiles(items); err != nil {
							r.logger.Debug().Msgf("Failed to delete broken items for %s: %v", m.Title, err)
						}

						// Search for missing items
						if err := a.SearchMissing(items); err != nil {
							r.logger.Debug().Msgf("Failed to search missing items for %s: %v", m.Title, err)
						}
					}

					mu.Lock()
					brokenItems = append(brokenItems, items...)
					mu.Unlock()
				}
			}
		}()
	}

	go func() {
		defer close(workerChan)
		for _, m := range media {
			select {
			case <-ctx.Done():
				return
			case workerChan <- m:
			}
		}
	}()

	wg.Wait()
	if len(brokenItems) == 0 {
		r.logger.Info().Msgf("No broken items found for %s[%s]", a.Name, strings.Join(job.MediaIDs, ","))
		return brokenItems, nil
	}

	r.logger.Info().Msgf("Repair completed for %s[%s]. %d broken items found", a.Name, strings.Join(job.MediaIDs, ","), len(brokenItems))
	return brokenItems, nil
}

func (r *Repair) getBrokenFiles(ctx context.Context, media arr.Content) []arr.ContentFile {
	if r.manager == nil {
		r.logger.Info().Msg("No manager found. Can't check broken files")
		return nil
	}

	brokenFiles := make([]arr.ContentFile, 0)
	mu := sync.Mutex{}
	uniqueParents := collectFiles(media)
	p := pool.New().
		WithContext(ctx).
		WithMaxGoroutines(r.workers)
	for torrentPath, files := range uniqueParents {
		p.Go(func(ctx context.Context) error {
			entryBrokenFiles := r.checkFiles(torrentPath, files)
			if len(entryBrokenFiles) > 0 {
				mu.Lock()
				brokenFiles = append(brokenFiles, entryBrokenFiles...)
				mu.Unlock()
			}
			return nil
		})
	}
	if err := p.Wait(); err != nil {
		r.logger.Error().Err(err).Msgf("Error checking files for %s", media.Title)
		return nil
	}
	if len(brokenFiles) == 0 {
		return nil
	}
	r.logger.Debug().Msgf("%d broken files found for %s", len(brokenFiles), media.Title)
	return brokenFiles
}
