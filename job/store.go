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
	wg   sync.WaitGroup // tracks active worker goroutines
}

func NewStore() *Store {
	return &Store{jobs: make(map[string]*Job)}
}

// IsProcessing returns true if any job is currently pending or processing.
func (s *Store) IsProcessing() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, j := range s.jobs {
		if j.Status == StatusPending || j.Status == StatusProcessing {
			return true
		}
	}
	return false
}

// Go launches fn in a tracked goroutine. Call Wait() to block until all finish.
func (s *Store) Go(fn func()) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		fn()
	}()
}

// Wait blocks until all goroutines launched via Go() have returned.
func (s *Store) Wait() {
	s.wg.Wait()
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
