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

const serialsPlaceholder = "{{SERIALS}}"

// DB holds configuration only — no live connection.
// Call Connect() to open a connection for a single job.
type DB struct {
	dsn          string
	query        string
	queryTimeout time.Duration
}

func New(dsn, query string, queryTimeoutSecs int) (*DB, error) {
	if !strings.Contains(query, serialsPlaceholder) {
		return nil, fmt.Errorf("query must contain %q placeholder for the IN clause", serialsPlaceholder)
	}
	return &DB{
		dsn:          dsn,
		query:        query,
		queryTimeout: time.Duration(queryTimeoutSecs) * time.Second,
	}, nil
}

// Conn is a live Oracle connection tied to one job.
// Always call Close() when done.
type Conn struct {
	conn         *sql.DB
	query        string
	queryTimeout time.Duration
}

func (d *DB) Connect(maxConns int) (*Conn, error) {
	conn, err := sql.Open("oracle", d.dsn)
	if err != nil {
		return nil, fmt.Errorf("open oracle connection: %w", err)
	}
	conn.SetMaxOpenConns(maxConns)
	conn.SetMaxIdleConns(maxConns)
	if err := conn.Ping(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("oracle ping failed: %w", err)
	}
	slog.Info("DB connection pool opened", "max_conns", maxConns, "query_timeout", d.queryTimeout)
	return &Conn{conn: conn, query: d.query, queryTimeout: d.queryTimeout}, nil
}

func (c *Conn) Close() {
	if err := c.conn.Close(); err != nil {
		slog.Error("failed to close DB connection", "error", err)
		return
	}
	slog.Info("DB connection closed")
}

// GetSerialRows returns map[serial_no]csvRow for found serials.
// Each row is pre-formatted as the columns defined in the configured query.
// The query is cancelled automatically if it exceeds the configured timeout.
func (c *Conn) GetSerialRows(serials []string) (map[string]string, error) {
	if len(serials) == 0 {
		return map[string]string{}, nil
	}

	placeholders := make([]string, len(serials))
	args := make([]any, len(serials))
	for i, s := range serials {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
		args[i] = s
	}

	query := strings.ReplaceAll(c.query, serialsPlaceholder, strings.Join(placeholders, ","))

	ctx, cancel := context.WithTimeout(context.Background(), c.queryTimeout)
	defer cancel()

	start := time.Now()
	rows, err := c.conn.QueryContext(ctx, query, args...)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("oracle query timed out after %s (serials=%d)", c.queryTimeout, len(serials))
		}
		return nil, fmt.Errorf("oracle query failed (serials=%d): %w", len(serials), err)
	}
	defer rows.Close()

	result := make(map[string]string, len(serials))
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

	slog.Info("DB query complete",
		"queried", len(serials),
		"found", len(result),
		"not_found", len(serials)-len(result),
		"duration_ms", time.Since(start).Milliseconds(),
		"timeout_ms", c.queryTimeout.Milliseconds(),
	)

	return result, nil
}
