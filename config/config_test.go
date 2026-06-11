package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_Defaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{}`), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ServerPort != "8080" {
		t.Errorf("ServerPort: got %q, want %q", cfg.ServerPort, "8080")
	}
	if cfg.UploadDir != "uploads" {
		t.Errorf("UploadDir: got %q, want %q", cfg.UploadDir, "uploads")
	}
	if cfg.ResultDir != "results" {
		t.Errorf("ResultDir: got %q, want %q", cfg.ResultDir, "results")
	}
	if cfg.StagingTable != "MUSANED_VOUCHERS" {
		t.Errorf("StagingTable: got %q, want %q", cfg.StagingTable, "MUSANED_VOUCHERS")
	}
	if cfg.InsertBatchSize != 10000 {
		t.Errorf("InsertBatchSize: got %d, want %d", cfg.InsertBatchSize, 10000)
	}
	if cfg.QueryTimeoutSecs != 120 {
		t.Errorf("QueryTimeoutSecs: got %d, want %d", cfg.QueryTimeoutSecs, 120)
	}
}

func TestLoad_Override(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	content := `{
		"server_port": "9090",
		"insert_batch_size": 500,
		"query_timeout_secs": 30,
		"job_ttl_secs": 7200
	}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ServerPort != "9090" {
		t.Errorf("ServerPort: got %q, want %q", cfg.ServerPort, "9090")
	}
	if cfg.InsertBatchSize != 500 {
		t.Errorf("InsertBatchSize: got %d, want %d", cfg.InsertBatchSize, 500)
	}
	if cfg.QueryTimeoutSecs != 30 {
		t.Errorf("QueryTimeoutSecs: got %d, want %d", cfg.QueryTimeoutSecs, 30)
	}
	if cfg.JobTTLSecs != 7200 {
		t.Errorf("JobTTLSecs: got %d, want %d", cfg.JobTTLSecs, 7200)
	}
	// Fields not in JSON should keep their defaults
	if cfg.UploadDir != "uploads" {
		t.Errorf("UploadDir: got %q, want %q (default should be preserved)", cfg.UploadDir, "uploads")
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load("/nonexistent/path/config.json")
	if err == nil {
		t.Error("expected error for missing file, got nil")
	}
}

func TestLoad_InvalidJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{invalid json`), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}
