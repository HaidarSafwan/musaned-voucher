package job

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusDone       Status = "done"
	StatusFailed     Status = "failed"
)

type Job struct {
	ID         string
	Status     Status
	Progress   int
	Error      string
	ResultPath string
	CreatedAt  time.Time
}

type Store struct {
	mu   sync.RWMutex
	jobs map[string]*Job
}

func NewStore() *Store {
	return &Store{jobs: make(map[string]*Job)}
}

func (s *Store) Create() *Job {
	j := &Job{ID: uuid.NewString(), Status: StatusPending, CreatedAt: time.Now()}
	s.mu.Lock()
	s.jobs[j.ID] = j
	s.mu.Unlock()
	return j
}

func (s *Store) Get(id string) (*Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	return j, ok
}

func (s *Store) Update(id string, fn func(*Job)) {
	s.mu.Lock()
	if j, ok := s.jobs[id]; ok {
		fn(j)
	}
	s.mu.Unlock()
}
