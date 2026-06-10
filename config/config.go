package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	OracleDSN   string `json:"oracle_dsn"`
	Query       string `json:"query"`
	ServerPort  string `json:"server_port"`
	UploadDir   string `json:"upload_dir"`
	ResultDir   string `json:"result_dir"`
	ChunkSize   int    `json:"chunk_size"`
	Parallelism int    `json:"parallelism"`
}

func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	cfg := &Config{
		ServerPort:  "8080",
		UploadDir:   "uploads",
		ResultDir:   "results",
		ChunkSize:   500,
		Parallelism: 4,
	}
	return cfg, json.NewDecoder(f).Decode(cfg)
}
