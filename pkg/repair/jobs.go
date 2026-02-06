package repair

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"golang.org/x/sync/errgroup"
)

func (r *Repair) AddJob(arrsNames []string, mediaIDs []string, autoProcess, recurrent bool) error {
	return r.addJob(r.ctx, arrsNames, mediaIDs, autoProcess, recurrent)
}

func (r *Repair) addJobWithContext(ctx context.Context, arrsNames []string, mediaIDs []string, autoProcess, recurrent bool) error {
	return r.addJob(ctx, arrsNames, mediaIDs, autoProcess, recurrent)
}

func (r *Repair) addJob(ctx context.Context, arrsNames []string, mediaIDs []string, autoProcess, recurrent bool) error {
	key := jobKey(arrsNames, mediaIDs)

	// Check for existing running job
	job := r.manager.Storage().GetRepairJobByUniqueKey(key)
	if job != nil && job.Status == storage.JobStarted {
		return fmt.Errorf("job already running")
	}
	job = r.newJob(arrsNames, mediaIDs)
	if job == nil {
		return fmt.Errorf("failed to create job")
	}
	job.AutoProcess = autoProcess
	job.Recurrent = recurrent
	job.ID = cmp.Or(job.ID, uuid.New().String())
	r.reset(job)

	ctx, cancelFunc := context.WithCancel(ctx)

	r.activeContexts.Store(job.ID, contexts{
		ctx:    ctx,
		cancel: cancelFunc,
	})

	// Save job
	if err := r.manager.Storage().SaveRepairJob(key, job); err != nil {
		return err
	}

	go func() {
		if err := r.repair(ctx, job); err != nil {
			r.logger.Error().Err(err).Msg("Error running repair")
			if !errors.Is(ctx.Err(), context.Canceled) {
				job.FailedAt = time.Now()
				job.Error = err.Error()
				job.Status = storage.JobFailed
				job.CompletedAt = time.Now()
			} else {
				job.FailedAt = time.Now()
				job.Error = err.Error()
				job.Status = storage.JobFailed
				job.CompletedAt = time.Now()
			}
		}
		r.onComplete() // Clear caches and maps after job completion
	}()
	return nil
}

func (r *Repair) StopJob(id string) error {
	job := r.GetJob(id)
	if job == nil {
		return fmt.Errorf("job %s not found", id)
	}

	// Check if job can be stopped
	if job.Status != storage.JobStarted && job.Status != storage.JobProcessing {
		return fmt.Errorf("job %s cannot be stopped (status: %s)", id, job.Status)
	}

	// Cancel the job
	if ctx, ok := r.activeContexts.Load(id); ok && ctx.cancel != nil {
		ctx.cancel()
	}
	go func() {
		if job.Status == storage.JobStarted || job.Status == storage.JobProcessing {
			job.Status = storage.JobCancelled
			job.BrokenItems = nil
			// Clear active context
			r.activeContexts.Delete(id)
			job.CompletedAt = time.Now()
			job.Error = "Job was cancelled by user"
			r.saveToStorage(job)
		}
	}()

	return fmt.Errorf("job %s cannot be cancelled", id)
}

func (r *Repair) GetJob(id string) *storage.Job {
	job, err := r.manager.Storage().GetRepairJob(id)
	if err != nil {
		r.logger.Error().Err(err).Msgf("Failed to get job %s", id)
		return nil
	}
	return job
}

func (r *Repair) GetJobs() []*storage.Job {
	jobs, err := r.manager.Storage().LoadAllRepairJobs()
	if err != nil {
		r.logger.Error().Err(err).Msg("Failed to load repair jobs")
		return nil
	}
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].StartedAt.After(jobs[j].StartedAt)
	})

	return jobs
}

func (r *Repair) ProcessJob(id string) error {
	job := r.GetJob(id)
	if job == nil {
		return fmt.Errorf("job %s not found", id)
	}
	if job.Status != storage.JobPending {
		return fmt.Errorf("job %s not pending", id)
	}
	if job.StartedAt.IsZero() {
		return fmt.Errorf("job %s not started", id)
	}
	if !job.CompletedAt.IsZero() {
		return fmt.Errorf("job %s already completed", id)
	}
	if !job.FailedAt.IsZero() {
		return fmt.Errorf("job %s already failed", id)
	}

	brokenItems := job.BrokenItems
	if len(brokenItems) == 0 {
		r.logger.Info().Msgf("No broken items found for job %s", id)
		job.CompletedAt = time.Now()
		job.Status = storage.JobCompleted
		return nil
	}

	ctxObj, ok := r.activeContexts.Load(id)
	if !ok {
		c, cancel := context.WithCancel(r.ctx)
		ctxObj = contexts{
			ctx:    c,
			cancel: cancel,
		}
		r.activeContexts.Store(id, ctxObj)
	}

	g, ctx := errgroup.WithContext(ctxObj.ctx)
	g.SetLimit(r.workers)

	for arrName, items := range brokenItems {
		g.Go(func() error {

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			a := r.manager.Arr().Get(arrName)
			if a == nil {
				r.logger.Error().Msgf("Arr %s not found", arrName)
				return nil
			}

			if err := a.DeleteFiles(items); err != nil {
				r.logger.Error().Err(err).Msgf("Failed to delete broken items for %s", arrName)
				return nil
			}
			// Search for missing items
			if err := a.SearchMissing(items); err != nil {
				r.logger.Error().Err(err).Msgf("Failed to search missing items for %s", arrName)
				return nil
			}
			return nil
		})
	}

	// Update job status to in-progress
	job.Status = storage.JobProcessing
	r.saveToStorage(job)

	// Launch a goroutine to wait for completion and update the job
	go func() {
		if err := g.Wait(); err != nil {
			job.FailedAt = time.Now()
			job.Error = err.Error()
			job.CompletedAt = time.Now()
			job.Status = storage.JobFailed
			r.logger.Error().Err(err).Msgf("Job %s failed", id)
		} else {
			job.CompletedAt = time.Now()
			job.Status = storage.JobCompleted
			r.logger.Info().Msgf("Job %s completed successfully", id)
		}

		r.saveToStorage(job)
	}()

	return nil
}

func (r *Repair) DeleteJobs(ids []string) {
	for _, id := range ids {
		if id == "" {
			continue
		}
		err := r.manager.Storage().DeleteRepairJob(id)
		if err != nil {
			r.logger.Error().Err(err).Msgf("Failed to delete job %s", id)
		}
	}
}
