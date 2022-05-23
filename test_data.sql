INSERT INTO gtl.job(job_name, job_cmd, job_cmd_args, job_sql, timeout_seconds, retries)
VALUES
    ('test_job_1', 'echo', '"test 1"', '', 3, 1)
,   ('test_job_2', '', '', 'SELECT 2', 7, 1)
,   ('test_job_3', 'echo', '"test 3"', '', 5, 1)
,   ('test_job_4', '', '', 'SELECT 4', 4, 1)
ON CONFLICT DO NOTHING
;
INSERT INTO gtl.schedule (schedule, min_seconds_between_attempts)
VALUES
    ('realtime', 5)
;
INSERT INTO gtl.job_schedule (job_name, schedule)
VALUES
    ('test_job_1', 'realtime')
,   ('test_job_2', 'realtime')
,   ('test_job_3', 'realtime')
,   ('test_job_4', 'realtime')
ON CONFLICT DO NOTHING
;
INSERT INTO gtl.job_dependency (job_name, dependency)
VALUES
    ('test_job_3', 'test_job_2')
,   ('test_job_4', 'test_job_3')
;

