package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"serial-enricher/config"
	"serial-enricher/db"
	"serial-enricher/handler"
	"serial-enricher/job"
	"syscall"
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
	slog.Info("config loaded", "port", cfg.ServerPort, "insert_batch_size", cfg.InsertBatchSize)
	if cfg.APIKey == "" || cfg.APIKey == "change-me-before-deploy" {
		slog.Error("api_key is not set — update config.json before running in production")
		os.Exit(1)
	}

	database, err := db.New(cfg.OracleDSN, cfg.StagingDSN, cfg.StagingTable, cfg.Query, cfg.QueryTimeoutSecs)
	if err != nil {
		slog.Error("invalid DB config", "error", err)
		os.Exit(1)
	}
	slog.Info("DB config validated",
		"staging_table",     cfg.StagingTable,
		"insert_batch_size", cfg.InsertBatchSize,
		"query_timeout_secs", cfg.QueryTimeoutSecs,
	)

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

	// Run server in background so main goroutine can wait for OS signal
	go func() {
		slog.Info("server starting", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	slog.Info("shutdown signal received", "signal", sig.String())

	// Stop accepting new HTTP requests (30s timeout)
	httpCtx, httpCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer httpCancel()
	if err := srv.Shutdown(httpCtx); err != nil {
		slog.Error("HTTP server shutdown error", "error", err)
	}
	slog.Info("HTTP server stopped")

	// Wait for all active jobs to finish (10 min timeout)
	workerDone := make(chan struct{})
	go func() {
		store.Wait()
		close(workerDone)
	}()

	select {
	case <-workerDone:
		slog.Info("all jobs completed, shutdown clean")
	case <-time.After(10 * time.Minute):
		slog.Warn("shutdown timeout reached, exiting with jobs still running")
	}
}
