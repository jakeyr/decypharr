package storage

import (
	"fmt"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/pkg/arr"
	"google.golang.org/protobuf/proto"
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
	Error       string                       `json:"error"`
}

func (j *Job) DiscordContext() string {
	format := `
		**ID**: %s
		**arrs**: %s
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
	pb := JobToProto(job)
	data, err := proto.Marshal(pb)
	if err != nil {
		return err
	}

	if err := s.repairJobs.Put(job.ID, data, nil); err != nil {
		return err
	}

	// Store unique key mapping if different
	if key != "" && key != job.ID {
		_ = s.repairKeys.Put(key, []byte(job.ID), nil)
	}
	return nil
}

// GetRepairJob retrieves a single repair job by ID
func (s *Storage) GetRepairJob(id string) (*Job, error) {
	data, err := s.repairJobs.Get(id)
	if err != nil {
		return nil, err
	}

	var pb JobProto
	if err := proto.Unmarshal(data, &pb); err != nil {
		return nil, err
	}
	return ProtoToJob(&pb), nil
}

// GetRepairJobByUniqueKey retrieves a job by unique key
func (s *Storage) GetRepairJobByUniqueKey(uniqueKey string) *Job {
	jobIDData, err := s.repairKeys.Get(uniqueKey)
	if err != nil {
		return nil
	}

	job, err := s.GetRepairJob(string(jobIDData))
	if err != nil {
		return nil
	}
	return job
}

// DeleteRepairJob removes a repair job by ID
func (s *Storage) DeleteRepairJob(id string) error {
	return s.repairJobs.Delete(id)
}

// SaveAllRepairJobs saves all repair jobs
func (s *Storage) SaveAllRepairJobs(jobs map[string]*Job) error {
	for key, job := range jobs {
		_ = s.SaveRepairJob(key, job)
	}
	return nil
}

// LoadAllRepairJobs loads all repair jobs
func (s *Storage) LoadAllRepairJobs() ([]*Job, error) {
	var jobs []*Job
	_ = s.repairJobs.ForEach(func(key string, value []byte) error {
		var pb JobProto
		if proto.Unmarshal(value, &pb) == nil {
			jobs = append(jobs, ProtoToJob(&pb))
		}
		return nil
	})
	return jobs, nil
}

// CountRepairJobs returns the total number of repair jobs
func (s *Storage) CountRepairJobs() (int, error) {
	return s.repairJobs.Len(), nil
}
