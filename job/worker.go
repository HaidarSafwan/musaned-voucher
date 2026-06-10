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
	"sync"
	"sync/atomic"
)

func Process(store *Store, jobID, inputPath, outputDir string, database *db.DB, chunkSize, parallelism int) {
	log := slog.With("job_id", jobID)
	log.Info("job started", "input", inputPath, "parallelism", parallelism)

	store.Update(jobID, func(j *Job) { j.Status = StatusProcessing })

	conn, err := database.Connect(parallelism)
	if err != nil {
		log.Error("failed to connect to DB", "error", err)
		store.Update(jobID, func(j *Job) { j.Status = StatusFailed; j.Error = err.Error() })
		return
	}
	defer conn.Close()

	// Single file pass: read all chunks and get total row count
	chunks, total, err := readAllChunks(inputPath, chunkSize)
	if err != nil {
		log.Error("failed to read input file", "error", err)
		store.Update(jobID, func(j *Job) { j.Status = StatusFailed; j.Error = err.Error() })
		return
	}
	log.Info("file read complete", "total_rows", total, "chunks", len(chunks))

	resultPath := fmt.Sprintf("%s/%s.csv", outputDir, jobID)
	outFile, err := os.Create(resultPath)
	if err != nil {
		log.Error("failed to create result file", "path", resultPath, "error", err)
		store.Update(jobID, func(j *Job) { j.Status = StatusFailed; j.Error = err.Error() })
		return
	}
	defer outFile.Close()

	// Buffered writer — flushes in 1MB blocks instead of one syscall per row
	writer := bufio.NewWriterSize(outFile, 1<<20)
	defer writer.Flush()
	writer.WriteString("serial_no,status,consumption_date,phone\n")

	// results[i] holds the DB records for chunks[i], preserving input order
	results := make([]map[string]string, len(chunks))

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		jobErr  error
		processed atomic.Int64
	)
	sem := make(chan struct{}, parallelism) // limits concurrent DB queries

	for i, chunk := range chunks {
		wg.Add(1)
		sem <- struct{}{} // acquire slot
		go func(idx int, ch []string) {
			defer wg.Done()
			defer func() { <-sem }() // release slot

			records, err := conn.GetSerialRows(ch)
			if err != nil {
				log.Error("DB query failed", "chunk_index", idx, "error", err)
				mu.Lock()
				if jobErr == nil {
					jobErr = err
				}
				mu.Unlock()
				return
			}

			results[idx] = records

			n := processed.Add(int64(len(ch)))
			store.Update(jobID, func(j *Job) {
				j.Progress = int(float64(n) / float64(total) * 100)
			})
			log.Info("chunk complete", "chunk_index", idx, "rows", len(ch), "processed", n, "total", total)
		}(i, chunk)
	}

	wg.Wait()

	if jobErr != nil {
		store.Update(jobID, func(j *Job) { j.Status = StatusFailed; j.Error = jobErr.Error() })
		return
	}

	// Write results in original CSV order
	for i, chunk := range chunks {
		for _, sn := range chunk {
			sn = strings.TrimSpace(sn)
			if row, found := results[i][sn]; found {
				writer.WriteString(row + "\n")
			} else {
				writer.WriteString(sn + ",not_found,,\n")
			}
		}
	}

	store.Update(jobID, func(j *Job) {
		j.Status = StatusDone
		j.Progress = 100
		j.ResultPath = resultPath
	})
	log.Info("job completed", "result", resultPath, "total_rows", total)
}

// readAllChunks reads the entire CSV in one pass and returns chunks + total row count.
// Replaces the separate countRows + streaming approach.
func readAllChunks(path string, chunkSize int) ([][]string, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	if _, err := r.Read(); err != nil { // skip header
		return nil, 0, fmt.Errorf("read header: %w", err)
	}

	var chunks [][]string
	total := 0
	for {
		chunk, done := readChunk(r, chunkSize)
		if len(chunk) > 0 {
			chunks = append(chunks, chunk)
			total += len(chunk)
		}
		if done {
			break
		}
	}
	return chunks, total, nil
}

func readChunk(r *csv.Reader, size int) ([]string, bool) {
	var chunk []string
	for i := 0; i < size; i++ {
		row, err := r.Read()
		if err == io.EOF {
			return chunk, true
		}
		if err != nil {
			slog.Warn("skipping malformed CSV row", "error", err)
			continue
		}
		if len(row) > 0 && row[0] != "" {
			chunk = append(chunk, row[0])
		}
	}
	return chunk, false
}
