package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	OracleDSN       string `json:"oracle_dsn"`        // DB that runs the SELECT query
	StagingDSN      string `json:"staging_dsn"`       // DB that holds the staging table
	StagingTable    string `json:"staging_table"`     // staging table name
	Query           string `json:"query"`             // SELECT query; filters by job_id via :1
	ServerPort      string `json:"server_port"`
	UploadDir       string `json:"upload_dir"`
	ResultDir       string `json:"result_dir"`
	InsertBatchSize int    `json:"insert_batch_size"` // rows per bulk INSERT batch
	APIKey          string `json:"api_key"`
	QueryTimeoutSecs int   `json:"query_timeout_secs"`
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
	}
	return cfg, json.NewDecoder(f).Decode(cfg)
}
