package handler

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"serial-enricher/config"
	"serial-enricher/db"
	"serial-enricher/job"
	"strings"

	"github.com/go-chi/chi/v5"
)

type Handler struct {
	store *job.Store
	db    *db.DB
	cfg   *config.Config
}

func New(store *job.Store, database *db.DB, cfg *config.Config) *Handler {
	return &Handler{store: store, db: database, cfg: cfg}
}

type jobRequest struct {
	File string `json:"file"`
}

// writeError sends a JSON error response and logs it at the appropriate level.
func writeError(w http.ResponseWriter, status int, msg string, logArgs ...any) {
	if status >= 500 {
		slog.Error(msg, logArgs...)
	} else {
		slog.Warn(msg, logArgs...)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// POST /api/jobs
func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	// Cap body at 20MB — handles base64 overhead of a 12MB CSV
	r.Body = http.MaxBytesReader(w, r.Body, 20<<20)

	var req jobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body", "error", err)
		return
	}
	if req.File == "" {
		writeError(w, http.StatusBadRequest, "missing file field")
		return
	}

	j := h.store.Create()
	inputPath := filepath.Join(h.cfg.UploadDir, j.ID+".csv")

	dst, err := os.Create(inputPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create upload file",
			"job_id", j.ID, "path", inputPath, "error", err)
		return
	}
	defer dst.Close()

	// Stream base64 decode directly to disk — no full decode in memory
	decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(req.File))
	if _, err := io.Copy(dst, decoder); err != nil {
		writeError(w, http.StatusBadRequest, "invalid base64 content",
			"job_id", j.ID, "error", err)
		return
	}

	slog.Info("job created", "job_id", j.ID, "input", inputPath)
	go job.Process(h.store, j.ID, inputPath, h.cfg.ResultDir, h.db, h.cfg.ChunkSize, h.cfg.Parallelism)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"job_id": j.ID})
}

// GET /api/jobs/{id}
func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	j, ok := h.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "job not found", "job_id", id)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(j); err != nil {
		slog.Error("failed to encode job response", "job_id", id, "error", err)
	}
}

// GET /api/jobs/{id}/result
func (h *Handler) DownloadResult(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	j, ok := h.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "job not found", "job_id", id)
		return
	}
	if j.Status == job.StatusFailed {
		writeError(w, http.StatusUnprocessableEntity, "job failed", "job_id", id, "reason", j.Error)
		return
	}
	if j.Status != job.StatusDone {
		writeError(w, http.StatusConflict, "job not ready",
			"job_id", id, "status", j.Status, "progress", j.Progress)
		return
	}

	slog.Info("result downloaded", "job_id", id)
	w.Header().Set("Content-Disposition", `attachment; filename="result.csv"`)
	w.Header().Set("Content-Type", "text/csv")
	http.ServeFile(w, r, j.ResultPath)

	// Clean up both files after the response is fully sent
	inputPath := filepath.Join(h.cfg.UploadDir, id+".csv")
	if err := os.Remove(inputPath); err != nil {
		slog.Warn("failed to delete upload file", "job_id", id, "path", inputPath, "error", err)
	} else {
		slog.Info("upload file deleted", "job_id", id, "path", inputPath)
	}
	if err := os.Remove(j.ResultPath); err != nil {
		slog.Warn("failed to delete result file", "job_id", id, "path", j.ResultPath, "error", err)
	} else {
		slog.Info("result file deleted", "job_id", id, "path", j.ResultPath)
	}
}
