package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

const dateFormat = "2006-01-02"

func deleteOldLogs() error {
	items, err := ioutil.ReadDir("logs")
	if err != nil {
		return fmt.Errorf(`ioutil.ReadDir("logs"): %v`, err)
	}

	for _, item := range items {
		if !item.IsDir() && strings.HasSuffix(item.Name(), ".txt") {
			dateStr := strings.Split(item.Name(), ".")[1]
			date, err := time.Parse(dateFormat, dateStr)
			if err != nil {
				return fmt.Errorf("time.Parse(dateFormat: %s, itemName(): %s): %v", dateFormat, item.Name(), err)
			}
			// only keep the last 3 days of logs
			if time.Since(date).Hours() > (3 * 24) {
				fullPath := filepath.Join("logs", item.Name())
				err := os.Remove(fullPath)
				if err != nil {
					return fmt.Errorf("os.Remove(%s): %v", fullPath, err)
				}
				log.Printf("Deleted %s.", fullPath)
			}
		}
	}

	return nil
}

func logError(
	ctx context.Context,
	pool *pgxpool.Pool,
	errorLog *log.Logger,
	batchId string,
	message string,
) {
	errorLog.Print(message)

	sql := "CALL gtl.log_error(p_batch_id := $1, p_message := $2)"
	_, err := pool.Exec(ctx, sql, batchId, message)
	if err != nil {
		errorLog.Printf("logError(...) -> CALL gtl.log_error(p_batch_id := '%s', p_message := '%s') -> %v", batchId, message, err)
	}
}

func openLogFile(prefix string) (*os.File, error) {
	path := filepath.Join("logs", fmt.Sprintf("%s.%s.txt", prefix, time.Now().Format(dateFormat)))
	handle, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	return handle, nil
}

func logInfo(
	ctx context.Context,
	pool *pgxpool.Pool,
	infoLog *log.Logger,
	errorLog *log.Logger,
	batchId string,
	message string,
) {
	infoLog.Print(message)

	sql := "CALL gtl.log_info(p_batch_id := $1, p_message := $2)"
	_, err := pool.Exec(ctx, sql, batchId, message)
	if err != nil {
		logError(
			ctx, pool, errorLog, batchId,
			fmt.Sprintf(
				"logInfo(...) -> CALL gtl.log_info(p_batch_id := '%s', p_message := '%s') -> %v",
				batchId, message, err,
			),
		)
	}
}

func logJobAddedToQueue(
	ctx context.Context,
	pool *pgxpool.Pool,
	errorLog *log.Logger,
	infoLog *log.Logger,
	job Job,
) {
	infoLog.Printf("<%s> added to queue.", job.Name)

	sql := "CALL gtl.job_added_to_queue(p_batch_id := $1, p_job_id := $2, p_job_name := $3)"
	_, err := pool.Exec(ctx, sql, job.BatchId, job.Id, job.Name)
	if err != nil {
		logJobError(
			ctx, pool, errorLog, job,
			fmt.Sprintf(
				"logJobAddedToQueue(...) -> con.Exec(ctx: ..., sql: %s, batchId: %s, jobId: %s, jobName: %s) -> %v",
				sql, job.BatchId, job.Id, job.Name, err,
			),
		)
	}
}

func logJobCancelled(
	ctx context.Context,
	pool *pgxpool.Pool,
	infoLog *log.Logger,
	errorLog *log.Logger,
	job Job,
) {
	infoLog.Printf("<%s> cancelled.", job.Name)

	sql := "CALL gtl.log_job_cancel(p_batch_id := $1, p_job_id := $2, p_job_name := $3)"
	_, err := pool.Exec(ctx, sql, job.BatchId, job.Id, job.Name)
	if err != nil {
		errorLog.Printf(
			"logJobCancelled(...) -> CALL gtl.log_job_cancel(p_batch_id := '%s', p_job_id := '%s', p_job_name := '%s') -> %v",
			job.BatchId, job.Id, job.Name, err,
		)
	}
}

func logJobEnded(
	ctx context.Context,
	pool *pgxpool.Pool,
	errorLog *log.Logger,
	infoLog *log.Logger,
	job Job,
	executionSeconds int,
) {
	infoLog.Printf("<%s> finished after %d seconds.", job.Name, executionSeconds)

	sql := "CALL gtl.job_ended(p_batch_id := $1, p_job_id := $2, p_job_name := $3)"
	_, err := pool.Exec(ctx, sql, job.BatchId, job.Id, job.Name)
	if err != nil {
		logJobError(
			ctx, pool, errorLog, job,
			fmt.Sprintf(
				"logJobEnded(...) -> con.Exec(ctx: ..., sql: %s, batchId: %s, jobId: %s, jobName: %s) -> %v",
				sql, job.BatchId, job.Id, job.Name, err,
			),
		)
	}
}

func logJobError(
	ctx context.Context,
	pool *pgxpool.Pool,
	errorLog *log.Logger,
	job Job,
	message string,
) {
	errorLog.Printf("<%s> %s", job.Name, message)

	sql := "CALL gtl.log_job_error(p_batch_id := $1, p_job_id := $2, p_job_name := $3, p_message := $4)"
	_, err := pool.Exec(ctx, sql, job.BatchId, job.Id, job.Name, message)
	if err != nil {
		errorLog.Printf(
			"logJobError(...) -> CALL gtl.log_job_error(p_batch_id := '%s', p_job_id := '%s', p_job_name := '%s', p_message := '%s') -> %v",
			job.BatchId, job.Id, job.Name, message, err,
		)
	}
}

func logJobInfo(
	ctx context.Context,
	pool *pgxpool.Pool,
	infoLog *log.Logger,
	errorLog *log.Logger,
	job Job,
	message string,
) {
	infoLog.Printf("<%s> %s", job.Name, message)

	sql := "CALL gtl.log_job_info(p_batch_id := $1, p_job_id := $2, p_job_name := $3, p_message := $4)"
	_, err := pool.Exec(ctx, sql, job.BatchId, job.Id, job.Name, message)
	if err != nil {
		errorLog.Printf(
			"logJobInfo(...) -> CALL gtl.log_job_info(p_batch_id := '%s', p_job_id := '%s', p_job_name := '%s', p_message := '%s') -> %v",
			job.BatchId, job.Id, job.Name, message, err,
		)
	}
}

func logJobStarted(
	ctx context.Context,
	pool *pgxpool.Pool,
	infoLog *log.Logger,
	errorLog *log.Logger,
	job Job,
) {
	infoLog.Printf("<%s> started.", job.Name)

	sql := "CALL gtl.job_started(p_batch_id := $1, p_job_id := $2, p_job_name := $3)"
	_, err := pool.Exec(ctx, sql, job.BatchId, job.Id, job.Name)
	if err != nil {
		logJobError(
			ctx, pool, errorLog, job,
			fmt.Sprintf(
				"logJobStarted(...) -> con.Exec(ctx: ..., sql: %s, batchId: %s, jobId: %s jobName: %s) -> %v",
				sql, job.BatchId, job.Id, job.Name, err,
			),
		)
	}
}

func logRunningJobsCancelled(
	ctx context.Context,
	pool *pgxpool.Pool,
	infoLog *log.Logger,
	errorLog *log.Logger,
	batchId string,
) {
	infoLog.Print("Running jobs cancelled.")

	sql := "CALL gtl.cancel_running_jobs()"
	_, err := pool.Exec(ctx, sql)
	if err != nil {
		logError(
			ctx, pool, errorLog, batchId, fmt.Sprintf("CALL gtl.cancel_running_jobs() -> %v", err),
		)
	}
}
