package job

import (
	"log/slog"
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
	UpdatedAt  time.Time
}

type Store struct {
	mu      sync.RWMutex
	jobs    map[string]*Job
	wg      sync.WaitGroup
	ttl     time.Duration
	closeCh chan struct{}
}

func NewStore(ttl time.Duration) *Store {
	s := &Store{
		jobs:    make(map[string]*Job),
		ttl:     ttl,
		closeCh: make(chan struct{}),
	}
	go s.cleanupLoop()
	return s
}

// Close stops the background cleanup goroutine. Call once during shutdown.
func (s *Store) Close() {
	close(s.closeCh)
}

func (s *Store) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.evictExpired()
		case <-s.closeCh:
			return
		}
	}
}

func (s *Store) evictExpired() {
	if s.ttl <= 0 {
		return
	}
	cutoff := time.Now().Add(-s.ttl)
	s.mu.Lock()
	defer s.mu.Unlock()
	evicted := 0
	for id, j := range s.jobs {
		if (j.Status == StatusDone || j.Status == StatusFailed) && j.UpdatedAt.Before(cutoff) {
			delete(s.jobs, id)
			evicted++
		}
	}
	if evicted > 0 {
		slog.Info("evicted expired jobs", "count", evicted, "ttl", s.ttl)
	}
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
	now := time.Now()
	j := &Job{ID: uuid.NewString(), Status: StatusPending, CreatedAt: now, UpdatedAt: now}
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
		j.UpdatedAt = time.Now()
	}
	s.mu.Unlock()
}

// List returns a point-in-time snapshot of all jobs currently in the store.
func (s *Store) List() []Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, *j)
	}
	return out
}
