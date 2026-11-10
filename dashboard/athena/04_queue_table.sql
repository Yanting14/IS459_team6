-- The BQ1 review queue as an Athena table, for the dashboard (DASH_SOURCE=athena).
-- Apply (also updates existing table locations):   python athena\run_sql.py athena\04_queue_table.sql
--
-- The score job writes date=<day>/accounts.csv, ranking.json and _SUCCESS side by side in each day's folder.
-- Athena skips _SUCCESS (names starting with "_" are ignored) but would read ranking.json as if it were
-- CSV, so the view keeps only accounts.csv through the hidden "$path" column. `date` is a partition column
-- projected for 1-30 Sep 2022; days with no folder are simply empty, so a new queue day needs no DDL change.
--
-- All columns are read as text (the CSV is quoted where a reason contains a comma). `score` stays text on
-- purpose: the dashboard shows it at full precision.
--
-- LOCATION is ONE pipeline execution. If the team publishes a newer execution, change the LOCATION and the
-- template in both CREATE and ALTER statements below, rerun this file, and set DASH_QUEUE_URI to the
-- same folder so ranking.json is read from it too. Restart Streamlit to clear cached results.

CREATE EXTERNAL TABLE IF NOT EXISTS is459_team6.queue_files (
  `rank`         string,
  bank_id        string,
  account_id     string,
  score          string,
  model_version  string,
  window_start   string,
  cutoff         string,
  reason_1       string,
  reason_2       string,
  reason_3       string
)
PARTITIONED BY (`date` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS TEXTFILE
LOCATION 's3://is459-team6-gold-207567757101/bq1/queue/bq1-bankvis-20261109/'
TBLPROPERTIES (
  'skip.header.line.count'        = '1',
  'projection.enabled'            = 'true',
  'projection.date.type'          = 'date',
  'projection.date.range'         = '2022-09-01,2022-09-30',
  'projection.date.format'        = 'yyyy-MM-dd',
  'projection.date.interval'      = '1',
  'projection.date.interval.unit' = 'DAYS',
  'storage.location.template'     = 's3://is459-team6-gold-207567757101/bq1/queue/bq1-bankvis-20261109/date=${date}/'
);

-- CREATE IF NOT EXISTS leaves existing tables unchanged; apply the configured location on reruns.
ALTER TABLE is459_team6.queue_files SET LOCATION 's3://is459-team6-gold-207567757101/bq1/queue/bq1-bankvis-20261109/';
ALTER TABLE is459_team6.queue_files SET TBLPROPERTIES (
  'storage.location.template' = 's3://is459-team6-gold-207567757101/bq1/queue/bq1-bankvis-20261109/date=${date}/'
);

CREATE OR REPLACE VIEW is459_team6.queue_accounts AS
SELECT CAST("rank" AS integer) AS "rank", bank_id, account_id, score, model_version, window_start, cutoff,
       reason_1, reason_2, reason_3, "date"
FROM is459_team6.queue_files
WHERE "$path" LIKE '%/accounts.csv';

-- Check: the top of one day's queue. Expect 15 Sep and 16 Sep to have rows; rank 1 first.
SELECT "rank", bank_id, account_id, score, reason_1
FROM is459_team6.queue_accounts
WHERE "date" = '2022-09-15'
ORDER BY "rank"
LIMIT 5;
