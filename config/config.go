package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	StagingDSN       string `json:"staging_dsn"`   // Oracle DB for INSERT, SELECT, and DELETE
	StagingTable     string `json:"staging_table"` // staging table name
	Query            string `json:"query"`         // SELECT query; filters by job_id via :1
	ServerPort       string `json:"server_port"`
	UploadDir        string `json:"upload_dir"`
	ResultDir        string `json:"result_dir"`
	InsertBatchSize  int    `json:"insert_batch_size"` // rows per bulk INSERT batch
	APIKey           string `json:"api_key"`
	QueryTimeoutSecs int    `json:"query_timeout_secs"`
	JobTTLSecs       int    `json:"job_ttl_secs"`        // seconds before done/failed jobs are evicted from memory
	StorePath        string `json:"store_path"`          // path to flat JSON job persistence file; empty = disabled
	RateLimitRPS     int    `json:"rate_limit_rps"`      // max requests per second per IP (0 = disabled)
	RateLimitBurst   int    `json:"rate_limit_burst"`    // burst allowance above RPS
}

func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	cfg := &Config{
		ServerPort:       "8080",
		UploadDir:        "uploads",
		ResultDir:        "results",
		StagingTable:     "MUSANED_VOUCHERS",
		InsertBatchSize:  10000,
		QueryTimeoutSecs: 120,
		JobTTLSecs:       10000,
		StorePath:        "jobs.json",
		RateLimitRPS:     10,
		RateLimitBurst:   20,
	}
	return cfg, json.NewDecoder(f).Decode(cfg)
}
