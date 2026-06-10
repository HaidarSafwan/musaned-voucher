package main

import (
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"serial-enricher/config"
	"serial-enricher/db"
	"serial-enricher/handler"
	"serial-enricher/job"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func apiKeyMiddleware(apiKey string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// CORS preflight carries no auth headers — let corsMiddleware handle it
			if r.Method == http.MethodOptions {
				next.ServeHTTP(w, r)
				return
			}
			key := r.Header.Get("X-API-Key")
			if key == "" {
				slog.Warn("missing API key", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"error":"missing X-API-Key header"}`))
				return
			}
			if key != apiKey {
				slog.Warn("invalid API key", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				w.Write([]byte(`{"error":"invalid API key"}`))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("panic recovered", "panic", rec, "method", r.Method, "path", r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// defaultConfigPath returns config.json in the same directory as the binary.
// Falls back to the current working directory if the executable path cannot be resolved.
func defaultConfigPath() string {
	exe, err := os.Executable()
	if err != nil {
		return "config.json"
	}
	return filepath.Join(filepath.Dir(exe), "config.json")
}

func main() {
	// Structured JSON logging to stdout
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfgPath := defaultConfigPath()
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		slog.Error("failed to load config", "path", cfgPath, "error", err)
		os.Exit(1)
	}
	slog.Info("config loaded", "port", cfg.ServerPort, "chunk_size", cfg.ChunkSize)
	if cfg.APIKey == "" || cfg.APIKey == "change-me-before-deploy" {
		slog.Error("api_key is not set — update config.json before running in production")
		os.Exit(1)
	}

	database, err := db.New(cfg.OracleDSN, cfg.Query)
	if err != nil {
		slog.Error("invalid DB config", "error", err)
		os.Exit(1)
	}
	slog.Info("DB config validated")

	if err := os.MkdirAll(cfg.UploadDir, 0755); err != nil {
		slog.Error("failed to create upload dir", "dir", cfg.UploadDir, "error", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(cfg.ResultDir, 0755); err != nil {
		slog.Error("failed to create result dir", "dir", cfg.ResultDir, "error", err)
		os.Exit(1)
	}

	store := job.NewStore()
	h := handler.New(store, database, cfg)

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(recoveryMiddleware)
	r.Use(corsMiddleware)
	r.Use(apiKeyMiddleware(cfg.APIKey))
	r.Post("/api/jobs", h.CreateJob)
	r.Get("/api/jobs/{id}", h.GetJob)
	r.Get("/api/jobs/{id}/result", h.DownloadResult)

	srv := &http.Server{
		Addr:         ":" + cfg.ServerPort,
		Handler:      r,
		ReadTimeout:  5 * time.Minute,
		WriteTimeout: 5 * time.Minute,
	}

	slog.Info("server starting", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
