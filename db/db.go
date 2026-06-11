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

// Ping opens a short-lived connection to verify the staging DB is reachable.
// Used by the /ready health check endpoint.
func (d *DB) Ping() error {
	conn, err := sql.Open("oracle", d.stagingDSN)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return conn.PingContext(ctx)
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
//
// Uses a manual timer + cancel rather than context.WithTimeout so that
// the deadline does not fire mid-scan between Oracle row-batch fetches.
func (c *Conn) GetAllRows(jobID string) (map[string]string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type scanResult struct {
		data map[string]string
		err  error
	}
	ch := make(chan scanResult, 1)
	start := time.Now()

	go func() {
		rows, err := c.conn.QueryContext(ctx, c.query, jobID)
		if err != nil {
			ch <- scanResult{err: fmt.Errorf("query failed: %w", err)}
			return
		}
		defer rows.Close()

		data := make(map[string]string)
		for rows.Next() {
			var row string
			if err := rows.Scan(&row); err != nil {
				ch <- scanResult{err: fmt.Errorf("scan row: %w", err)}
				return
			}
			if idx := strings.Index(row, ","); idx > 0 {
				data[row[:idx]] = row
			}
		}
		if err := rows.Err(); err != nil {
			ch <- scanResult{err: fmt.Errorf("rows iteration: %w", err)}
			return
		}
		ch <- scanResult{data: data}
	}()

	timer := time.NewTimer(c.queryTimeout)
	defer timer.Stop()

	select {
	case r := <-ch:
		if r.err == nil {
			slog.Info("query complete",
				"job_id",      jobID,
				"found",       len(r.data),
				"duration_ms", time.Since(start).Milliseconds(),
			)
		}
		return r.data, r.err
	case <-timer.C:
		cancel() // signal Oracle to stop sending rows
		return nil, fmt.Errorf("query timed out after %s", c.queryTimeout)
	}
}

// Cleanup truncates the staging table. Safe because the service enforces
// single-job-at-a-time, so no other job's rows are ever present concurrently.
// TRUNCATE is orders of magnitude faster than DELETE for large row counts
// because it deallocates blocks rather than generating per-row undo.
func (c *Conn) Cleanup(jobID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := c.conn.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", c.stagingTable))
	if err != nil {
		return fmt.Errorf("staging truncate failed: %w", err)
	}
	slog.Info("staging truncate complete", "job_id", jobID)
	return nil
}
