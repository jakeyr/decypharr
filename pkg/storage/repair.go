package storage

import (
	"fmt"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"
)

type JobStatus string

const (
	JobStarted    JobStatus = "started"
	JobPending    JobStatus = "pending"
	JobFailed     JobStatus = "failed"
	JobCompleted  JobStatus = "completed"
	JobProcessing JobStatus = "processing"
	JobCancelled  JobStatus = "cancelled"
)

type Job struct {
	ID          string                       `json:"id"`
	Arrs        []string                     `json:"arrs"`
	MediaIDs    []string                     `json:"media_ids"`
	StartedAt   time.Time                    `json:"created_at"`
	BrokenItems map[string][]arr.ContentFile `json:"broken_items"`
	Status      JobStatus                    `json:"status"`
	CompletedAt time.Time                    `json:"finished_at"`
	FailedAt    time.Time                    `json:"failed_at"`
	AutoProcess bool                         `json:"auto_process"`
	Recurrent   bool                         `json:"recurrent"`

	Error string `json:"error"`
}

func (j *Job) DiscordContext() string {
	format := `
		**ID**: %s
		**Arrs**: %s
		**Media IDs**: %s
		**Status**: %s
		**Started At**: %s
		**Completed At**: %s
`

	dateFmt := "2006-01-02 15:04:05"

	return fmt.Sprintf(format, j.ID, strings.Join(j.Arrs, ","), strings.Join(j.MediaIDs, ", "), j.Status, j.StartedAt.Format(dateFmt), j.CompletedAt.Format(dateFmt))
}

// SaveRepairJob saves a single repair job
func (s *Storage) SaveRepairJob(key string, job *Job) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(repairBucket))
		if bucket == nil {
			return fmt.Errorf("repair bucket not found")
		}

		data, err := msgpack.Marshal(job)
		if err != nil {
			return fmt.Errorf("failed to marshal repair job: %w", err)
		}

		if err := bucket.Put([]byte(job.ID), data); err != nil {
			return err
		}

		// Also index by key if different from ID

		if repairUniqueBucket := tx.Bucket([]byte(repairUniqueBucket)); repairUniqueBucket == nil {
			_ = repairUniqueBucket.Put([]byte(key), []byte(job.ID))
		}
		return nil
	})
}

// GetRepairJob retrieves a single repair job by key
func (s *Storage) GetRepairJob(key string) (*Job, error) {
	var job Job

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(repairBucket))
		if bucket == nil {
			return fmt.Errorf("repair bucket not found")
		}

		data := bucket.Get([]byte(key))
		if data == nil {
			return fmt.Errorf("job not found: %s", key)
		}

		return msgpack.Unmarshal(data, &job)
	})

	return &job, err
}

func (s *Storage) GetRepairJobByUniqueKey(uniqueKey string) *Job {
	var job Job

	_ = s.db.View(func(tx *bolt.Tx) error {
		uniqueBucket := tx.Bucket([]byte(repairUniqueBucket))
		if uniqueBucket == nil {
			return fmt.Errorf("repair unique bucket not found")
		}

		jobID := uniqueBucket.Get([]byte(uniqueKey))
		if jobID == nil {
			return fmt.Errorf("job not found for unique key: %s", uniqueKey)
		}

		bucket := tx.Bucket([]byte(repairBucket))
		if bucket == nil {
			return fmt.Errorf("repair bucket not found")
		}

		data := bucket.Get(jobID)
		if data == nil {
			return fmt.Errorf("job not found: %s", string(jobID))
		}

		return msgpack.Unmarshal(data, &job)
	})

	return &job
}

// DeleteRepairJob removes a repair job by key
func (s *Storage) DeleteRepairJob(id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(repairBucket))
		if bucket == nil {
			return fmt.Errorf("repair bucket not found")
		}

		return bucket.Delete([]byte(id))
	})
}

// SaveAllRepairJobs saves all repair jobs at once
func (s *Storage) SaveAllRepairJobs(jobs map[string]*Job) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(repairBucket))
		if bucket == nil {
			return fmt.Errorf("repair bucket not found")
		}

		for key, job := range jobs {
			data, err := msgpack.Marshal(job)
			if err != nil {
				s.logger.Warn().Err(err).Str("key", key).Msg("Failed to marshal repair job")
				continue
			}

			if err := bucket.Put([]byte(key), data); err != nil {
				s.logger.Warn().Err(err).Str("key", key).Msg("Failed to save repair job")
			}
		}
		return nil
	})
}

// LoadAllRepairJobs loads all repair jobs from storage
func (s *Storage) LoadAllRepairJobs() ([]*Job, error) {
	jobs := make([]*Job, 0)

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(repairBucket))
		if bucket == nil {
			return fmt.Errorf("repair bucket not found")
		}

		return bucket.ForEach(func(k, v []byte) error {
			var job Job
			if err := msgpack.Unmarshal(v, &job); err != nil {
				s.logger.Warn().Err(err).Str("key", string(k)).Msg("Failed to unmarshal repair job")
				return nil // Skip corrupted entries
			}
			jobs = append(jobs, &job)
			return nil
		})
	})

	return jobs, err
}

// CountRepairJobs returns the total number of repair jobs
func (s *Storage) CountRepairJobs() (int, error) {
	count := 0

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(repairBucket))
		if bucket == nil {
			return fmt.Errorf("repair bucket not found")
		}

		stats := bucket.Stats()
		count = stats.KeyN
		return nil
	})

	return count, err
}
