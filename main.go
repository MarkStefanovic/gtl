package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

func main() {
	fmt.Println("Starting gtl.  Press the Enter key to stop it.")

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	err := os.MkdirAll("logs", 0755)
	if err != nil {
		log.Fatalf("os.MkdirAll()(")
	}

	errorFile, err := openLogFile("error")
	if err != nil {
		log.Fatalf("openLogFile failed: %v", err)
	}

	errorLog := log.New(
		io.MultiWriter(errorFile, os.Stdout),
		"ERROR: ",
		log.Ldate|log.Ltime|log.Lshortfile,
	)

	infoLog := log.New(
		os.Stdout,
		"INFO: ",
		log.Ldate|log.Ltime|log.Lshortfile,
	)

	err = deleteOldLogs()
	if err != nil {
		errorLog.Fatalf("An error occurred while deleting old logs -> %v", err)
	}

	rand.Seed(time.Now().UnixNano())

	batchId := createUUID()

	connectionString, err := getConnectionString()
	if err != nil {
		errorLog.Fatalf("An error occurred while reading the config file, config.json -> %v", err)
	}

	pool, err := pgxpool.Connect(context.Background(), connectionString)
	if err != nil {
		errorLog.Fatalf("Unable to connect to database: %v", err)
	}
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logRunningJobsCancelled(context.Background(), pool, infoLog, errorLog, batchId)

	waitGroup := sync.WaitGroup{}

	waitGroup.Add(2)

	jobQueue := make(chan Job)
	defer close(jobQueue)

	go jobScheduler(ctx, &waitGroup, pool, infoLog, errorLog, jobQueue, batchId, 5)

	go jobRunner(ctx, &waitGroup, pool, infoLog, errorLog, jobQueue, batchId)

	// wait for the user to press the Enter key
	go func() {
		_, err := os.Stdin.Read(make([]byte, 1))
		if err != nil {
			logError(ctx, pool, errorLog, batchId, fmt.Sprintf("An error occurred while reading Stdin -> %v", err))
			os.Exit(1)
		}
		cancel()
	}()

	waitGroup.Wait()

	logInfo(context.Background(), pool, infoLog, errorLog, batchId, "gtl cancelled.")
}

func getReadyJobs(ctx context.Context, pool *pgxpool.Pool, batchId string, maxJobs int) ([]Job, error) {
	sql := "SELECT t.job_name, t.job_cmd, t.job_cmd_args, t.job_sql, t.retries FROM gtl.get_ready_jobs(max_jobs := $1) AS t;"
	rows, err := pool.Query(ctx, sql, maxJobs)
	if err != nil {
		return []Job{}, fmt.Errorf("pool.Query(ctx: ..., sql: %s, maxJobs: %d) -> %v", sql, maxJobs, err)
	}
	defer rows.Close()

	var readyJobs []Job
	for rows.Next() {
		var jobName string
		var cmd string
		var cmdArgs string
		var sql string
		var retries int
		if err := rows.Scan(
			&jobName,
			&cmd,
			&cmdArgs,
			&sql,
			&retries,
		); err != nil {
			return nil, fmt.Errorf("rows.Scan(...) -> %v", err)
		}

		job := Job{
			BatchId: batchId,
			Id:      createUUID(),
			Name:    jobName,
			Cmd:     cmd,
			CmdArgs: cmdArgs,
			SQL:     sql,
			Retries: retries,
		}

		readyJobs = append(readyJobs, job)
	}

	return readyJobs, nil
}

func jobRunner(
	ctx context.Context,
	wg *sync.WaitGroup,
	pool *pgxpool.Pool,
	infoLog *log.Logger,
	errorLog *log.Logger,
	jobQueue <-chan Job,
	batchId string,
) {
	defer wg.Done()

	for {
		//fmt.Println("number of goroutines:", runtime.NumGoroutine())
		select {
		case <-ctx.Done():
			logInfo(context.Background(), pool, infoLog, errorLog, batchId, "jobRunner closed.")
			return
		case job, ok := <-jobQueue:
			if !ok {
				logInfo(context.Background(), pool, infoLog, errorLog, batchId, "jobQueue has been closed")
				return
			}

			go func() {
				start := time.Now()

				jobContext, jobCancel := context.WithTimeout(context.Background(), time.Second*5)
				defer jobCancel()

				done := make(chan error, 1)

				logJobStarted(jobContext, pool, infoLog, errorLog, job)

				go func() {
					done <- runJobWithRetry(jobContext, pool, infoLog, errorLog, job)
				}()

				select {
				case <-ctx.Done():
					logJobCancelled(context.Background(), pool, infoLog, errorLog, job)
				case <-done:
					logJobEnded(context.Background(), pool, errorLog, infoLog, job, int(time.Since(start).Seconds()))
				case <-jobContext.Done():
					logJobError(
						ctx, pool, errorLog, job,
						fmt.Sprintf("stopped after %d seconds -> %v", int(time.Since(start).Seconds()), jobContext.Err()),
					)
				}
			}()
		}

		time.Sleep(1 * time.Second)
	}
}

func jobScheduler(
	ctx context.Context,
	wg *sync.WaitGroup,
	pool *pgxpool.Pool,
	infoLog *log.Logger,
	errorLog *log.Logger,
	jobQueue chan<- Job,
	batchId string,
	maxJobs int,
) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			logInfo(context.Background(), pool, infoLog, errorLog, batchId, "jobScheduler closed.")
			return
		default:
			readyJobs, err := getReadyJobs(ctx, pool, batchId, maxJobs)
			if err != nil {
				logError(ctx, pool, errorLog, batchId, fmt.Sprintf("getReadyJobs(%d) -> %v", maxJobs, err))
			}

			for _, job := range readyJobs {
				select {
				case <-ctx.Done():
					logInfo(context.Background(), pool, infoLog, errorLog, batchId, "jobScheduler stopped.")
					return
				case jobQueue <- job:
					logJobAddedToQueue(ctx, pool, errorLog, infoLog, job)
				}
			}

			time.Sleep(1 * time.Second)
		}
	}
}

func runJobWithRetry(
	ctx context.Context,
	pool *pgxpool.Pool,
	infoLog *log.Logger,
	errorLog *log.Logger,
	job Job,
) error {
	var err error
	for i := 0; i < job.Retries+1; i++ {
		if i > 0 {
			logJobInfo(ctx, pool, infoLog, errorLog, job, fmt.Sprintf("running retry %d...", i))
		}
		select {
		case <-ctx.Done():
			return err
		default:
			err = runJob(ctx, pool, infoLog, errorLog, job)
			if err != nil {
				logJobError(ctx, pool, errorLog, job, err.Error())
			} else {
				return nil
			}
		}
	}

	return err
}

func runJob(
	ctx context.Context,
	pool *pgxpool.Pool,
	infoLog *log.Logger,
	errorLog *log.Logger,
	job Job,
) error {
	if len(job.Cmd) > 0 {
		out, err := exec.Command(job.Cmd, job.CmdArgs).Output()
		if err != nil {
			logJobError(ctx, pool, errorLog, job, err.Error())
			return err
		}

		logJobInfo(ctx, pool, infoLog, errorLog, job, string(out))
	} else {
		_, err := pool.Exec(ctx, job.SQL)
		if err != nil {
			logJobError(
				ctx, pool, errorLog, job,
				fmt.Sprintf("runJob(..., job: %v) -> pool.Exec(ctx, '%s') -> %v", job, job.SQL, err),
			)
		}
	}

	return nil
}

//func runJob(
//	ctx context.Context,
//	pool *pgxpool.Pool,
//	infoLog *log.Logger,
//	errorLog *log.Logger,
//	job Job,
//) error {
//	n := rand.Intn(10)
//	if n >= 9 {
//		return fmt.Errorf("rolled a %d", n)
//	} else {
//		logJobInfo(ctx, pool, infoLog, errorLog, job, fmt.Sprintf("rolled a %d", n))
//		time.Sleep(time.Duration(n) * time.Second)
//		return nil
//	}
//}
