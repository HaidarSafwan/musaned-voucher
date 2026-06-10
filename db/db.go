package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	_ "github.com/sijms/go-ora/v2"
)

// DB holds configuration only — no live connection.
// All operations (INSERT, SELECT, DELETE) run on the staging DB.
// Call Connect() to open a connection for a single job.
type DB struct {
	stagingDSN   string
	stagingTable string
	query        string
	queryTimeout time.Duration
}

func New(stagingDSN, stagingTable, query string, queryTimeoutSecs int) (*DB, error) {
	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("query must not be empty")
	}
	if strings.TrimSpace(stagingTable) == "" {
		return nil, fmt.Errorf("staging_table must not be empty")
	}
	return &DB{
		stagingDSN:   stagingDSN,
		stagingTable: stagingTable,
		query:        query,
		queryTimeout: time.Duration(queryTimeoutSecs) * time.Second,
	}, nil
}

// Conn is a live connection to the staging DB for one job.
// It handles INSERT, SELECT, and DELETE — all on the same connection.
// Always call Close() when done.
type Conn struct {
	conn         *sql.DB
	stagingTable string
	query        string
	queryTimeout time.Duration
}

func (d *DB) Connect() (*Conn, error) {
	conn, err := sql.Open("oracle", d.stagingDSN)
	if err != nil {
		return nil, fmt.Errorf("open staging connection: %w", err)
	}
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	if err := conn.Ping(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("staging DB ping failed: %w", err)
	}
	slog.Info("staging DB connection opened", "dsn", d.stagingDSN)
	return &Conn{
		conn:         conn,
		stagingTable: d.stagingTable,
		query:        d.query,
		queryTimeout: d.queryTimeout,
	}, nil
}

func (c *Conn) Close() {
	if err := c.conn.Close(); err != nil {
		slog.Error("failed to close staging DB connection", "error", err)
		return
	}
	slog.Info("staging DB connection closed")
}

// BulkInsert loads all serial numbers into the staging table in batches.
// Uses Oracle array binding — each batch is a single network round trip.
func (c *Conn) BulkInsert(jobID string, serials []string, batchSize int) error {
	tx, err := c.conn.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf(
		"INSERT INTO %s (job_id, serial_no) VALUES (:1, :2)", c.stagingTable,
	))
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare insert: %w", err)
	}
	defer stmt.Close()

	total := len(serials)
	for start := 0; start < total; start += batchSize {
		end := start + batchSize
		if end > total {
			end = total
		}
		batch := serials[start:end]

		jobIDs := make([]string, len(batch))
		for i := range jobIDs {
			jobIDs[i] = jobID
		}

		if _, err := stmt.Exec(jobIDs, batch); err != nil {
			tx.Rollback()
			return fmt.Errorf("bulk insert batch [%d:%d]: %w", start, end, err)
		}
		slog.Info("insert batch complete", "rows", len(batch), "total_inserted", end)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit bulk insert: %w", err)
	}
	slog.Info("bulk insert complete", "job_id", jobID, "total_rows", total)
	return nil
}

// GetAllRows runs the configured SELECT query on the staging DB,
// filtered by job_id. The query accesses vm_voucher via a DB link.
// Returns map[serial_no]csvRow — each row pre-formatted by the query.
func (c *Conn) GetAllRows(jobID string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.queryTimeout)
	defer cancel()

	start := time.Now()
	rows, err := c.conn.QueryContext(ctx, c.query, jobID)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("query timed out after %s", c.queryTimeout)
		}
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		if idx := strings.Index(row, ","); idx > 0 {
			result[row[:idx]] = row
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	slog.Info("query complete",
		"job_id",      jobID,
		"found",       len(result),
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return result, nil
}

// Cleanup deletes all staging rows for the given job_id.
func (c *Conn) Cleanup(jobID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	res, err := c.conn.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE job_id = :1", c.stagingTable),
		jobID,
	)
	if err != nil {
		return fmt.Errorf("staging cleanup failed: %w", err)
	}
	n, _ := res.RowsAffected()
	slog.Info("staging cleanup complete", "job_id", jobID, "rows_deleted", n)
	return nil
}
