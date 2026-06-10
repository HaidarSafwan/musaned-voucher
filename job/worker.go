package job

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"os"
	"serial-enricher/db"
	"strings"
)

func Process(store *Store, jobID, inputPath, outputDir string, database *db.DB, insertBatchSize int) {
	log := slog.With("job_id", jobID)
	log.Info("job started", "input", inputPath)

	store.Update(jobID, func(j *Job) { j.Status = StatusProcessing })

	conn, err := database.Connect()
	if err != nil {
		log.Error("failed to connect to DB", "error", err)
		store.Update(jobID, func(j *Job) { j.Status = StatusFailed; j.Error = err.Error() })
		return
	}
	defer conn.Close()

	// 1. Read all serial numbers from CSV in one pass
	serials, err := readAllSerials(inputPath)
	if err != nil {
		log.Error("failed to read input file", "error", err)
		store.Update(jobID, func(j *Job) { j.Status = StatusFailed; j.Error = err.Error() })
		return
	}
	log.Info("serials loaded", "count", len(serials))
	store.Update(jobID, func(j *Job) { j.Progress = 10 })

	// 2. Bulk insert serials into staging table
	if err := conn.BulkInsert(jobID, serials, insertBatchSize); err != nil {
		log.Error("bulk insert failed", "error", err)
		store.Update(jobID, func(j *Job) { j.Status = StatusFailed; j.Error = err.Error() })
		conn.Cleanup(jobID) // best-effort cleanup on failure
		return
	}
	store.Update(jobID, func(j *Job) { j.Progress = 40 })

	// 3. Single SELECT query joining vm_voucher with staging table
	records, err := conn.GetAllRows(jobID)
	if err != nil {
		log.Error("query failed", "error", err)
		store.Update(jobID, func(j *Job) { j.Status = StatusFailed; j.Error = err.Error() })
		conn.Cleanup(jobID)
		return
	}
	store.Update(jobID, func(j *Job) { j.Progress = 80 })

	// 4. Write enriched CSV in original input order
	resultPath := fmt.Sprintf("%s/%s.csv", outputDir, jobID)
	if err := writeResult(resultPath, serials, records); err != nil {
		log.Error("failed to write result", "error", err)
		store.Update(jobID, func(j *Job) { j.Status = StatusFailed; j.Error = err.Error() })
		conn.Cleanup(jobID)
		return
	}
	store.Update(jobID, func(j *Job) { j.Progress = 95 })

	// 5. Delete staging rows — always runs even if write succeeded
	if err := conn.Cleanup(jobID); err != nil {
		log.Warn("staging cleanup failed", "error", err)
		// non-fatal: result is ready, just log the cleanup failure
	}

	store.Update(jobID, func(j *Job) {
		j.Status = StatusDone
		j.Progress = 100
		j.ResultPath = resultPath
	})
	log.Info("job completed",
		"result",      resultPath,
		"total_rows",  len(serials),
		"found_in_db", len(records),
	)
}

func readAllSerials(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	if _, err := r.Read(); err != nil { // skip header
		return nil, fmt.Errorf("read header: %w", err)
	}

	var serials []string
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			slog.Warn("skipping malformed CSV row", "error", err)
			continue
		}
		if len(row) > 0 && row[0] != "" {
			serials = append(serials, strings.TrimSpace(row[0]))
		}
	}
	return serials, nil
}

func writeResult(path string, serials []string, records map[string]string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 1<<20)
	defer w.Flush()

	w.WriteString("serial_no,status,consumption_date,phone\n")
	for _, sn := range serials {
		if row, found := records[sn]; found {
			w.WriteString(row + "\n")
		} else {
			w.WriteString(sn + ",not_found,,\n")
		}
	}
	return nil
}
