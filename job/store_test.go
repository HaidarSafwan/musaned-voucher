package job

import (
	"testing"
	"time"
)

func TestStore_CreateAndGet(t *testing.T) {
	s := NewStore(time.Hour, "")
	defer s.Close()

	j := s.Create()
	if j.ID == "" {
		t.Error("expected non-empty job ID")
	}
	if j.Status != StatusPending {
		t.Errorf("status: got %q, want %q", j.Status, StatusPending)
	}
	if j.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set on Create")
	}
	if j.UpdatedAt.IsZero() {
		t.Error("UpdatedAt should be set on Create")
	}

	got, ok := s.Get(j.ID)
	if !ok {
		t.Fatal("expected job to be found after Create")
	}
	if got.ID != j.ID {
		t.Errorf("ID mismatch: got %q, want %q", got.ID, j.ID)
	}
}

func TestStore_GetMissing(t *testing.T) {
	s := NewStore(time.Hour, "")
	defer s.Close()

	_, ok := s.Get("does-not-exist")
	if ok {
		t.Error("expected not found for unknown ID")
	}
}

func TestStore_Update(t *testing.T) {
	s := NewStore(time.Hour, "")
	defer s.Close()

	j := s.Create()
	before := j.UpdatedAt

	time.Sleep(time.Millisecond) // ensure UpdatedAt advances

	s.Update(j.ID, func(j *Job) {
		j.Status = StatusProcessing
		j.Progress = 50
	})

	got, _ := s.Get(j.ID)
	if got.Status != StatusProcessing {
		t.Errorf("status: got %q, want %q", got.Status, StatusProcessing)
	}
	if got.Progress != 50 {
		t.Errorf("progress: got %d, want %d", got.Progress, 50)
	}
	if !got.UpdatedAt.After(before) {
		t.Error("UpdatedAt should advance after Update")
	}
}

func TestStore_Update_NoopOnMissing(t *testing.T) {
	s := NewStore(time.Hour, "")
	defer s.Close()

	// Should not panic on unknown ID
	s.Update("nonexistent", func(j *Job) { j.Status = StatusDone })
}

func TestStore_IsProcessing_EmptyStore(t *testing.T) {
	s := NewStore(time.Hour, "")
	defer s.Close()

	if s.IsProcessing() {
		t.Error("expected false with empty store")
	}
}

func TestStore_IsProcessing_Pending(t *testing.T) {
	s := NewStore(time.Hour, "")
	defer s.Close()

	s.Create() // StatusPending
	if !s.IsProcessing() {
		t.Error("expected true with a pending job")
	}
}

func TestStore_IsProcessing_Processing(t *testing.T) {
	s := NewStore(time.Hour, "")
	defer s.Close()

	j := s.Create()
	s.Update(j.ID, func(j *Job) { j.Status = StatusProcessing })
	if !s.IsProcessing() {
		t.Error("expected true with a processing job")
	}
}

func TestStore_IsProcessing_FalseWhenDone(t *testing.T) {
	s := NewStore(time.Hour, "")
	defer s.Close()

	j := s.Create()
	s.Update(j.ID, func(j *Job) { j.Status = StatusDone })
	if s.IsProcessing() {
		t.Error("expected false after job is done")
	}
}

func TestStore_IsProcessing_FalseWhenFailed(t *testing.T) {
	s := NewStore(time.Hour, "")
	defer s.Close()

	j := s.Create()
	s.Update(j.ID, func(j *Job) { j.Status = StatusFailed })
	if s.IsProcessing() {
		t.Error("expected false after job failed")
	}
}

func TestStore_EvictExpired_RemovesDoneJob(t *testing.T) {
	s := NewStore(time.Hour, "")
	defer s.Close()

	j := s.Create()
	s.Update(j.ID, func(j *Job) { j.Status = StatusDone })

	// Backdate UpdatedAt past the TTL
	s.mu.Lock()
	s.jobs[j.ID].UpdatedAt = time.Now().Add(-2 * time.Hour)
	s.mu.Unlock()

	s.evictExpired()

	if _, ok := s.Get(j.ID); ok {
		t.Error("expected expired done job to be evicted")
	}
}

func TestStore_EvictExpired_RemovesFailedJob(t *testing.T) {
	s := NewStore(time.Hour, "")
	defer s.Close()

	j := s.Create()
	s.Update(j.ID, func(j *Job) { j.Status = StatusFailed })

	s.mu.Lock()
	s.jobs[j.ID].UpdatedAt = time.Now().Add(-2 * time.Hour)
	s.mu.Unlock()

	s.evictExpired()

	if _, ok := s.Get(j.ID); ok {
		t.Error("expected expired failed job to be evicted")
	}
}

func TestStore_EvictExpired_SkipsActiveJobs(t *testing.T) {
	s := NewStore(time.Hour, "")
	defer s.Close()

	j := s.Create() // StatusPending

	// Even if outdated, active jobs must never be evicted
	s.mu.Lock()
	s.jobs[j.ID].UpdatedAt = time.Now().Add(-2 * time.Hour)
	s.mu.Unlock()

	s.evictExpired()

	if _, ok := s.Get(j.ID); !ok {
		t.Error("active job should not be evicted regardless of age")
	}
}

func TestStore_EvictExpired_SkipsRecentDoneJob(t *testing.T) {
	s := NewStore(time.Hour, "")
	defer s.Close()

	j := s.Create()
	s.Update(j.ID, func(j *Job) { j.Status = StatusDone })
	// UpdatedAt is recent — should not be evicted yet

	s.evictExpired()

	if _, ok := s.Get(j.ID); !ok {
		t.Error("recently completed job should not be evicted before TTL")
	}
}

func TestStore_EvictExpired_DisabledWithZeroTTL(t *testing.T) {
	s := NewStore(0, "") // TTL disabled
	defer s.Close()

	j := s.Create()
	s.Update(j.ID, func(j *Job) { j.Status = StatusDone })

	s.mu.Lock()
	s.jobs[j.ID].UpdatedAt = time.Now().Add(-24 * time.Hour)
	s.mu.Unlock()

	s.evictExpired()

	if _, ok := s.Get(j.ID); !ok {
		t.Error("job should not be evicted when TTL is 0 (disabled)")
	}
}

func TestStore_List_Empty(t *testing.T) {
	s := NewStore(time.Hour, "")
	defer s.Close()

	if jobs := s.List(); len(jobs) != 0 {
		t.Errorf("expected empty list, got %d jobs", len(jobs))
	}
}

func TestStore_List_Count(t *testing.T) {
	s := NewStore(time.Hour, "")
	defer s.Close()

	s.Create()
	s.Create()
	s.Create()

	if jobs := s.List(); len(jobs) != 3 {
		t.Errorf("expected 3 jobs, got %d", len(jobs))
	}
}

func TestStore_List_ReturnsCopies(t *testing.T) {
	s := NewStore(time.Hour, "")
	defer s.Close()

	j := s.Create()
	list := s.List()

	// Mutating the returned copy must not affect the store
	list[0].Status = StatusDone

	got, _ := s.Get(j.ID)
	if got.Status != StatusPending {
		t.Error("modifying a list copy should not affect the store")
	}
}
