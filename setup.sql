/*
DROP SCHEMA IF EXISTS gtl CASCADE;
*/
CREATE SCHEMA IF NOT EXISTS gtl;

CREATE TABLE IF NOT EXISTS gtl.job (
    job_id SERIAL PRIMARY KEY
,   job_name TEXT NOT NULL
,   job_cmd TEXT NOT NULL
,   job_cmd_args TEXT NOT NULL
,   job_sql TEXT NOT NULL
,   retries INT NOT NULL CHECK (retries >= 0)
,   timeout_seconds BIGINT NOT NULL CHECK (timeout_seconds > 0)
,   enabled BOOL NOT NULL DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS ix_job_job_name ON gtl.job (job_name);

CREATE TABLE IF NOT EXISTS gtl.job_dependency (
    job_name TEXT NOT NULL
,   dependency TEXT NOT NULL
,   PRIMARY KEY (job_name, dependency)
);

CREATE TABLE IF NOT EXISTS gtl.job_schedule (
    job_name TEXT NOT NULL
,   schedule TEXT NOT NULL
,   PRIMARY KEY (job_name, schedule)
);

CREATE TABLE IF NOT EXISTS gtl.schedule (
    schedule TEXT NOT NULL
,   start_ts TIMESTAMPTZ(0) NOT NULL DEFAULT '1900-01-01 +0'
,   end_ts TIMESTAMPTZ(0) NOT NULL DEFAULT '9999-12-31 +0'
,   start_month INT NOT NULL DEFAULT 1 CHECK (start_month BETWEEN 1 AND 12)
,   end_month INT NOT NULL DEFAULT 12 CHECK (end_month BETWEEN 1 AND 12)
,   start_month_day INT NOT NULL DEFAULT 1 CHECK (start_month_day BETWEEN 1 AND 31)
,   end_month_day INT NOT NULL DEFAULT 31 CHECK (end_month_day BETWEEN 1 AND 31)
,   start_week_day INT NOT NULL DEFAULT 1 CHECK (start_week_day BETWEEN 1 AND 7) -- Monday
,   end_week_day INT NOT NULL DEFAULT 7 CHECK (end_week_day BETWEEN 1 AND 7) -- Sunday
,   start_hour INT NOT NULL DEFAULT 1 CHECK (start_hour BETWEEN 1 AND 23)
,   end_hour INT NOT NULL DEFAULT 23 CHECK (end_hour BETWEEN 1 AND 23)
,   start_minute INT NOT NULL DEFAULT 1 CHECK (start_minute BETWEEN 1 AND 59)
,   end_minute INT NOT NULL DEFAULT 59 CHECK (end_minute BETWEEN 1 AND 59)
,   min_seconds_between_attempts BIGINT NOT NULL CHECK (min_seconds_between_attempts > 0)
,   PRIMARY KEY (schedule)
);

CREATE TABLE IF NOT EXISTS gtl.batch_start (
    batch_id CHAR(32) NOT NULL
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
,   PRIMARY KEY (batch_id)
);

CREATE TABLE IF NOT EXISTS gtl.job_cancel (
    batch_id CHAR(32) NOT NULL
,   job_id CHAR(32) NOT NULL
,   job_name TEXT NOT NULL
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
,   PRIMARY KEY (job_id, batch_id)
);
CREATE INDEX IF NOT EXISTS ix_job_cancel_job_name_ts ON gtl.job_cancel (job_name, ts DESC);

CREATE TABLE IF NOT EXISTS gtl.job_start (
    batch_id CHAR(32) NOT NULL
,   job_id CHAR(32) NOT NULL
,   job_name TEXT NOT NULL
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
,   PRIMARY KEY (job_id, batch_id)
);
CREATE INDEX IF NOT EXISTS ix_job_start_job_name_ts ON gtl.job_start (job_name, ts DESC);

CREATE TABLE IF NOT EXISTS gtl.job_end (
    batch_id CHAR(32) NOT NULL
,   job_id CHAR(32) NOT NULL
,   job_name TEXT NOT NULL
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
,   PRIMARY KEY (job_id, batch_id)
);
CREATE INDEX IF NOT EXISTS ix_job_end_job_name_ts ON gtl.job_end (job_name, ts DESC);

CREATE TABLE IF NOT EXISTS gtl.error (
    batch_id CHAR(32) NOT NULL
,   message TEXT NOT NULL
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
,   PRIMARY KEY (batch_id)
);

CREATE TABLE IF NOT EXISTS gtl.info (
    id SERIAL PRIMARY KEY
,   batch_id CHAR(32) NOT NULL
,   message TEXT NOT NULL
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_info_batch_id ON gtl.info (batch_id);

CREATE TABLE IF NOT EXISTS gtl.job_info (
    id SERIAL PRIMARY KEY
,   batch_id CHAR(32) NOT NULL
,   job_id CHAR(32) NOT NULL
,   job_name TEXT NOT NULL
,   message TEXT NOT NULL
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_job_info_batch_id ON gtl.job_info (job_id, batch_id);
CREATE INDEX IF NOT EXISTS ix_job_info_job_name_ts ON gtl.job_info (job_name, ts DESC);

CREATE TABLE IF NOT EXISTS gtl.job_error (
    id SERIAL PRIMARY KEY
,   batch_id CHAR(32) NOT NULL
,   job_id CHAR(32) NOT NULL
,   job_name TEXT NOT NULL
,   message TEXT NOT NULL
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_job_error_job_id_batch_id ON gtl.job_error (job_id, batch_id);
CREATE INDEX IF NOT EXISTS ix_job_error_job_name_ts ON gtl.job_error (job_name, ts DESC);

CREATE TABLE IF NOT EXISTS gtl.job_queue (
    batch_id CHAR(32) NOT NULL
,   job_id CHAR(32) NOT NULL
,   job_name TEXT NOT NULL
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
,   PRIMARY KEY (job_id, batch_id)
);

CREATE OR REPLACE PROCEDURE gtl.cancel_running_jobs() AS
$$
    INSERT INTO gtl.job_cancel (
        batch_id
    ,   job_id
    ,   job_name
    )
    SELECT
        s.batch_id
    ,   s.job_id
    ,   s.job_name
    FROM gtl.job_start AS s
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM gtl.job_end AS e
            WHERE
                s.batch_id = e.batch_id
                AND s.job_id = e.job_id
        )
        AND NOT EXISTS (
            SELECT 1
            FROM gtl.job_cancel AS c
            WHERE
                s.batch_id = c.batch_id
                AND s.job_id = c.job_id
        )
        AND NOT EXISTS (
            SELECT 1
            FROM gtl.job_error AS e
            WHERE
                s.batch_id = e.batch_id
                AND s.job_id = e.job_id
        )
    ;
$$
LANGUAGE sql;

CREATE OR REPLACE PROCEDURE gtl.job_added_to_queue (
    p_batch_id TEXT
,   p_job_id TEXT
,   p_job_name TEXT
)
AS $$
    INSERT INTO gtl.job_queue (batch_id, job_id, job_name)
    VALUES (p_batch_id, p_job_id, p_job_name);
$$
LANGUAGE sql;

CREATE OR REPLACE PROCEDURE gtl.job_started (
    p_batch_id TEXT
,   p_job_id TEXT
,   p_job_name TEXT
)
AS $$
    INSERT INTO gtl.job_start (batch_id, job_id, job_name)
    VALUES (p_batch_id, p_job_id, p_job_name)
    ON CONFLICT (job_id, batch_id) DO NOTHING;
$$
LANGUAGE sql;

CREATE OR REPLACE PROCEDURE gtl.job_ended (
    p_batch_id TEXT
,   p_job_id TEXT
,   p_job_name TEXT
)
AS $$
    INSERT INTO gtl.job_end (batch_id, job_id, job_name)
    VALUES (p_batch_id, p_job_id, p_job_name)
    ON CONFLICT (job_id, batch_id) DO NOTHING;
$$
LANGUAGE sql;

CREATE OR REPLACE PROCEDURE gtl.log_error(
    p_batch_id TEXT
,   p_message TEXT
) AS $$
    INSERT INTO gtl.error (batch_id, message)
    VALUES (p_batch_id, p_message);
$$
LANGUAGE sql;

CREATE OR REPLACE PROCEDURE gtl.log_info(
    p_batch_id TEXT
,   p_message TEXT
) AS $$
    INSERT INTO gtl.info (batch_id, message)
    VALUES (p_batch_id, p_message);
$$
LANGUAGE sql;

CREATE OR REPLACE PROCEDURE gtl.log_job_cancel(
    p_batch_id TEXT
,   p_job_id TEXT
,   p_job_name TEXT
) AS $$
    INSERT INTO gtl.job_cancel (batch_id, job_id, job_name)
    VALUES (p_batch_id, p_job_id, p_job_name);
$$
LANGUAGE sql;

CREATE OR REPLACE PROCEDURE gtl.log_job_error(
    p_batch_id TEXT
,   p_job_id TEXT
,   p_job_name TEXT
,   p_message TEXT
) AS $$
    INSERT INTO gtl.job_error (batch_id, job_id, job_name, message)
    VALUES (p_batch_id, p_job_id, p_job_name, p_message);
$$
LANGUAGE sql;

CREATE OR REPLACE PROCEDURE gtl.log_job_info(
    p_batch_id TEXT
,   p_job_id TEXT
,   p_job_name TEXT
,   p_message TEXT
) AS $$
    INSERT INTO gtl.job_info (batch_id, job_id, job_name, message)
    VALUES (p_batch_id, p_job_id, p_job_name, p_message);
$$
LANGUAGE sql;

CREATE OR REPLACE FUNCTION gtl.get_ready_jobs (
    max_jobs INT
)
RETURNS TABLE (
    job_name TEXT
,   job_cmd TEXT
,   job_cmd_args TEXT
,   job_sql TEXT
,   retries INT
)
AS $$
    WITH latest_job_runs AS (
        SELECT DISTINCT ON (s.job_id)
            s.job_id
        FROM gtl.job_start AS s
        ORDER BY
            s.job_id
        ,   s.ts DESC
    )
    , running_jobs AS (
        SELECT DISTINCT
            s.job_name
        FROM gtl.job AS j
        JOIN gtl.job_start AS s
            ON j.job_name = s.job_name
        JOIN latest_job_runs AS ljr
            ON s.job_id = ljr.job_id
        WHERE
            EXTRACT(EPOCH FROM NOW() - s.ts) < (j.timeout_seconds + 120)
            AND NOT EXISTS (
                SELECT 1
                FROM gtl.job_end AS e
                WHERE
                    s.batch_id = e.batch_id
                    AND s.job_id = e.job_id
            )
            AND NOT EXISTS (
                SELECT 1
                FROM gtl.job_cancel AS c
                WHERE
                    s.batch_id = c.batch_id
                    AND s.job_id = c.job_id
            )
            AND NOT EXISTS (
                SELECT 1
                FROM gtl.job_error AS e
                WHERE
                    s.batch_id = e.batch_id
                    AND s.job_id = e.job_id
            )
    )
    , latest_attempts AS (
        SELECT
            s.job_name
        ,   MAX(s.ts) AS ts
        FROM gtl.job_start AS s
        GROUP BY
            s.job_name
    )
    , latest_successful_runs AS (
        SELECT
            e.job_name
        ,   MAX(e.ts) AS ts
        FROM gtl.job_end AS e
        GROUP BY
            e.job_name
    )
    , latest_completions AS (
        SELECT
            t.job_name
        ,   MAX(t.ts) AS ts
        FROM (
            SELECT
                e.job_name
            ,   e.ts
            FROM latest_successful_runs AS e

            UNION

            SELECT
                e.job_name
            ,   MAX(e.ts) AS ts
            FROM gtl.job_error AS e
            GROUP BY
                e.job_name
        ) AS t
        GROUP BY
            t.job_name
    )
    , ready_jobs AS (
        SELECT
            js.job_name
        ,   j.job_cmd
        ,   j.job_cmd_args
        ,   j.job_sql
        ,   j.retries
        ,   la.ts AS latest_attempt
        ,   EXTRACT(EPOCH FROM now() - la.ts) AS seconds_since_latest_attempt
        ,   s.min_seconds_between_attempts
        FROM gtl.job_schedule AS js
        JOIN gtl.job AS j
            ON js.job_name = j.job_name
        JOIN gtl.schedule AS s
            ON js.schedule = s.schedule
        LEFT JOIN latest_attempts AS la
            ON js.job_name = la.job_name
        LEFT JOIN latest_completions AS lc
            ON js.job_name = lc.job_name
        WHERE
            j.enabled
            AND now() BETWEEN s.start_ts AND s.end_ts
            AND EXTRACT(MONTH FROM now()) BETWEEN s.start_month AND s.end_month
            AND EXTRACT(ISODOW FROM now()) BETWEEN s.start_week_day AND s.end_week_day
            AND EXTRACT(DAY FROM now()) BETWEEN s.start_month_day AND s.end_month_day
            AND EXTRACT(HOUR FROM now()) BETWEEN s.start_hour AND s.end_hour
            AND EXTRACT(MINUTE FROM now()) BETWEEN s.start_minute AND s.end_minute
            AND (
                NOT EXISTS (
                    SELECT 1
                    FROM running_jobs AS rj
                    WHERE js.job_name = rj.job_name
                )
                OR EXTRACT(EPOCH FROM now() - la.ts) > j.timeout_seconds + 60
            )
            AND (
                EXTRACT(EPOCH FROM now() - lc.ts) > s.min_seconds_between_attempts
                OR lc.job_name IS NULL
            )
            AND (
                NOT EXISTS (
                    SELECT 1
                    FROM gtl.job_dependency AS jd
                    WHERE
                        js.job_name = jd.job_name
                )
                OR EXISTS (
                    SELECT 1
                    FROM gtl.job_dependency AS jd
                    JOIN latest_successful_runs AS lsr
                        ON jd.dependency = lsr.job_name
                    WHERE
                        js.job_name = jd.job_name
                        AND (
                            lsr.ts > la.ts
                            OR la.ts IS NULL
                        )
                )
            )
    )
    , ready_deps AS (
        SELECT DISTINCT
            jd.job_name
        ,   jd.dependency
        FROM gtl.job_dependency AS jd
        JOIN ready_jobs AS rj
            ON jd.job_name = rj.job_name
    )
    SELECT
        rj.job_name
    ,   rj.job_cmd
    ,   rj.job_cmd_args
    ,   rj.job_sql
    ,   rj.retries
    FROM ready_jobs AS rj
    , LATERAL (
        SELECT
            CASE
                WHEN
                    EXISTS (
                        SELECT 1
                        FROM ready_deps AS rd
                        WHERE
                            rj.job_name = rd.job_name
                    )
                    AND NOT EXISTS (
                        SELECT 1
                        FROM ready_jobs AS rj2
                        JOIN ready_deps AS rd
                            ON rj2.job_name = rd.job_name
                        WHERE
                            rj.job_name = rd.dependency
                    )
                    THEN 4
                WHEN rj.seconds_since_latest_attempt > (rj.min_seconds_between_attempts * 4) THEN 3
                WHEN rj.seconds_since_latest_attempt > (rj.min_seconds_between_attempts * 3) THEN 2
                WHEN rj.seconds_since_latest_attempt > (rj.min_seconds_between_attempts * 2) THEN 1
                ELSE 0
            END AS priority
    ) AS p
    ORDER BY
        p.priority DESC
    ,   rj.latest_attempt
    LIMIT GREATEST((max_jobs - (SELECT COUNT(*) FROM running_jobs)), 0);
$$
LANGUAGE sql;

CREATE OR REPLACE PROCEDURE gtl.delete_old_log_entries(
    rows_deleted INOUT BIGINT = 0
) AS
$$
    DECLARE
        v_cutoff TIMESTAMPTZ(0) = now() - INTERVAL '3 DAYS';
    BEGIN
        WITH batch_error AS (
            DELETE FROM gtl.error AS b
            WHERE
                b.ts < v_cutoff
                -- don't delete the most recent batch
                AND EXISTS (
                    SELECT 1
                    FROM gtl.error AS b
                    WHERE b.ts > b.ts
                )
            RETURNING 1
        )
        , batch_info AS (
            DELETE FROM gtl.info AS b
            WHERE
                b.ts < v_cutoff
                -- don't delete the most recent batch
                AND EXISTS (
                    SELECT 1
                    FROM gtl.info AS b
                    WHERE b.ts > b.ts
                )
            RETURNING 1
        )
        , job_start AS (
            DELETE FROM gtl.job_start AS j
            WHERE
                j.ts < v_cutoff
                AND EXISTS (
                    SELECT 1
                    FROM gtl.job_start AS js
                    WHERE
                        j.job_name = js.job_name
                        AND js.ts > j.ts
                )
            RETURNING 1
        )
        , job_error AS (
            DELETE FROM gtl.job_error AS j
            WHERE
                j.ts < v_cutoff
                AND EXISTS (
                    SELECT 1
                    FROM gtl.job_error AS js
                    WHERE
                        j.job_name = js.job_name
                        AND js.ts > j.ts
                )
            RETURNING 1
        )
        , job_info AS (
            DELETE FROM gtl.job_info AS j
            WHERE
                j.ts < v_cutoff
                AND EXISTS (
                    SELECT 1
                    FROM gtl.job_info AS js
                    WHERE
                        j.job_name = js.job_name
                        AND js.ts > j.ts
                )
            RETURNING 1
        )
        , job_queue AS (
            DELETE FROM gtl.job_queue AS j
            WHERE
                j.ts < v_cutoff
            RETURNING 1
        )
        SELECT
            (SELECT COUNT(*) FROM batch_error)
        +   (SELECT COUNT(*) FROM batch_info)
        +   (SELECT COUNT(*) FROM job_start)
        +   (SELECT COUNT(*) FROM job_error)
        +   (SELECT COUNT(*) FROM job_info)
        +   (SELECT COUNT(*) FROM job_queue)
        INTO rows_deleted;
    END;
$$
LANGUAGE plpgsql;

INSERT INTO gtl.schedule (schedule, min_seconds_between_attempts)
VALUES ('hourly', 3600);

INSERT INTO gtl.job_schedule (job_name, schedule)
VALUES ('delete_old_logs', 'hourly')
ON CONFLICT DO NOTHING;

INSERT INTO gtl.job(job_name, job_cmd, job_cmd_args, job_sql, timeout_seconds, retries)
VALUES ('delete_old_logs', '', '', 'CALL gtl.delete_old_log_entries();', 900, 1)
ON CONFLICT DO NOTHING;