package job

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
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
	path    string // path to flat JSON persistence file; empty = disabled
	closeCh chan struct{}
}

func NewStore(ttl time.Duration, path string) *Store {
	s := &Store{
		jobs:    make(map[string]*Job),
		ttl:     ttl,
		path:    path,
		closeCh: make(chan struct{}),
	}
	s.load()
	go s.cleanupLoop()
	return s
}

// Close stops the background cleanup goroutine. Call once during shutdown.
func (s *Store) Close() {
	close(s.closeCh)
}

// load reads persisted jobs from disk on startup.
// In-flight jobs (pending/processing) are marked failed — their goroutines are gone.
// Expired jobs are dropped so the file does not grow unbounded across restarts.
func (s *Store) load() {
	if s.path == "" {
		return
	}
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		slog.Warn("store: failed to read persistence file", "path", s.path, "error", err)
		return
	}

	var jobs []*Job
	if err := json.Unmarshal(data, &jobs); err != nil {
		slog.Warn("store: failed to parse persistence file", "path", s.path, "error", err)
		return
	}

	now := time.Now()
	loaded := 0
	for _, j := range jobs {
		// Drop expired terminal jobs so the file doesn't accumulate forever
		if s.ttl > 0 && (j.Status == StatusDone || j.Status == StatusFailed) {
			if j.UpdatedAt.Before(now.Add(-s.ttl)) {
				continue
			}
		}
		// Jobs that were in-flight cannot be resumed — mark them failed
		if j.Status == StatusPending || j.Status == StatusProcessing {
			j.Status = StatusFailed
			j.Error = "service restarted while job was in progress"
			j.UpdatedAt = now
		}
		s.jobs[j.ID] = j
		loaded++
	}
	if loaded > 0 {
		slog.Info("store: jobs restored from disk", "path", s.path, "count", loaded)
	}
}

// persist atomically writes the current job store to disk.
// Uses write-to-temp + rename to prevent partial writes on crash.
func (s *Store) persist() {
	if s.path == "" {
		return
	}

	s.mu.RLock()
	jobs := make([]Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, *j)
	}
	s.mu.RUnlock()

	data, err := json.Marshal(jobs)
	if err != nil {
		slog.Error("store: marshal failed", "error", err)
		return
	}

	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		slog.Error("store: failed to write temp file", "path", tmp, "error", err)
		return
	}
	if err := os.Rename(tmp, s.path); err != nil {
		slog.Error("store: failed to rename temp file", "error", err)
	}
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
	evicted := 0
	for id, j := range s.jobs {
		if (j.Status == StatusDone || j.Status == StatusFailed) && j.UpdatedAt.Before(cutoff) {
			delete(s.jobs, id)
			evicted++
		}
	}
	s.mu.Unlock()
	if evicted > 0 {
		slog.Info("evicted expired jobs", "count", evicted, "ttl", s.ttl)
		s.persist()
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
	s.persist()
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
	s.persist()
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
