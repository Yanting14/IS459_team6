-- OPTIONAL: the BQ2 results (bank view vs shared view) as an Athena table, plus the matching demo query.
-- Run:   python athena\run_sql.py athena\03_sharing_table.sql --database is459_team6
--
-- The sharing/ folder holds two CSVs (comparison.csv and comparison_by_day.csv), and Athena reads every file in a
-- folder, so the view keeps only comparison.csv using the hidden "$path" column.

CREATE EXTERNAL TABLE IF NOT EXISTS is459_team6.sharing_files (
  bank_id             string,
  `view`              string,
  k                   int,
  reviewed            int,
  eligible            int,
  positive_accounts   int,
  true_positives      int,
  precision_at_k      double,
  recall_at_k         double,
  model               string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://is459-team6-gold-207567757101/ibm-aml/e2e-20261105-152512/sharing/'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE OR REPLACE VIEW is459_team6.sharing_comparison AS
SELECT bank_id, "view", k, reviewed, eligible, positive_accounts, true_positives, precision_at_k, recall_at_k, model
FROM is459_team6.sharing_files
WHERE "$path" LIKE '%/comparison.csv';

-- How much more does sharing catch? Expect bank view 165 of 320 reviewed (precision 51.6%, recall 30.6%)
-- and shared view 210 of 320 (65.6%, 38.9%), as in bq2/runs/README.md.
SELECT "view",
       sum(true_positives)                                        AS true_positives,
       sum(reviewed)                                              AS reviewed,
       round(100.0 * sum(true_positives) / sum(reviewed), 1)      AS precision_pct,
       round(100.0 * sum(true_positives) / sum(positive_accounts), 1) AS recall_pct
FROM is459_team6.sharing_comparison
GROUP BY "view"
ORDER BY "view";
