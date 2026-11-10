-- IS459 team 6: Athena tables over the pipeline's outputs (no data is copied; Athena reads the files in S3).
-- Run once:   python athena\run_sql.py athena\01_create_tables.sql
--
-- Column names and types follow prepare/prepare.py. Spark writes `transfers` and `transfer_labels` into
-- date=YYYY-MM-DD folders, so `date` is a partition column. Partition projection tells Athena the days
-- directly, so there is no MSCK REPAIR step and no crawler. If the curated prefix changes, edit the
-- three LOCATION lines (and the template) below.

CREATE DATABASE IF NOT EXISTS is459_team6;

CREATE EXTERNAL TABLE IF NOT EXISTS is459_team6.transfers (
  `timestamp`              timestamp,
  sender_bank_id           string,
  sender_account_number    string,
  receiver_bank_id         string,
  receiver_account_number  string,
  amount_received          double,
  receiving_currency       string,
  amount_paid              double,
  payment_currency         string,
  payment_format           string,
  transfer_id              string,
  source_file              string,
  source_sha256            string,
  source_row_id            bigint
)
PARTITIONED BY (`date` string)
STORED AS PARQUET
LOCATION 's3://is459-team6-curated-207567757101/ibm-aml/e2e-20261105-152512/transfers/'
TBLPROPERTIES (
  'projection.enabled'            = 'true',
  'projection.date.type'          = 'date',
  'projection.date.range'         = '2022-09-01,2022-09-30',
  'projection.date.format'        = 'yyyy-MM-dd',
  'projection.date.interval'      = '1',
  'projection.date.interval.unit' = 'DAYS',
  'storage.location.template'     = 's3://is459-team6-curated-207567757101/ibm-aml/e2e-20261105-152512/transfers/date=${date}'
);

-- Ground truth, kept apart from the transfers on purpose (ADR-0002). Join on transfer_id.
CREATE EXTERNAL TABLE IF NOT EXISTS is459_team6.transfer_labels (
  transfer_id      string,
  source_file      string,
  source_sha256    string,
  source_row_id    bigint,
  laundering_mark  int
)
PARTITIONED BY (`date` string)
STORED AS PARQUET
LOCATION 's3://is459-team6-curated-207567757101/ibm-aml/e2e-20261105-152512/transfer_labels/'
TBLPROPERTIES (
  'projection.enabled'            = 'true',
  'projection.date.type'          = 'date',
  'projection.date.range'         = '2022-09-01,2022-09-30',
  'projection.date.format'        = 'yyyy-MM-dd',
  'projection.date.interval'      = '1',
  'projection.date.interval.unit' = 'DAYS',
  'storage.location.template'     = 's3://is459-team6-curated-207567757101/ibm-aml/e2e-20261105-152512/transfer_labels/date=${date}'
);

-- One row per transfer listed in the dataset's patterns file, with its ring and pattern type.
-- Not partitioned: `date` is an ordinary column here.
CREATE EXTERNAL TABLE IF NOT EXISTS is459_team6.rings (
  ring_id                  bigint,
  pattern_type             string,
  `timestamp`              timestamp,
  sender_bank_id           string,
  sender_account_number    string,
  receiver_bank_id         string,
  receiver_account_number  string,
  amount_received          double,
  receiving_currency       string,
  amount_paid              double,
  payment_currency         string,
  payment_format           string,
  laundering_mark          int,
  `date`                   date
)
STORED AS PARQUET
LOCATION 's3://is459-team6-curated-207567757101/ibm-aml/e2e-20261105-152512/rings/';
