-- The two views the dashboard reads. Terraform (infra/athena.tf) applies this file after it creates the tables,
-- so nobody runs it by hand. Glue views are Presto-encoded JSON, which Terraform cannot write cleanly, so the
-- views go through Athena DDL instead. They hold no S3 paths: the tables underneath carry those.
--
-- queue_files and sharing_files read every file in their folder; the "$path" filter keeps one file each.

CREATE OR REPLACE VIEW is459_team6.queue_accounts AS
SELECT CAST("rank" AS integer) AS "rank", bank_id, account_id, score, model_version, window_start, cutoff,
       reason_1, reason_2, reason_3, "date"
FROM is459_team6.queue_files
WHERE "$path" LIKE '%/accounts.csv';

CREATE OR REPLACE VIEW is459_team6.sharing_comparison AS
SELECT bank_id, "view", k, reviewed, eligible, positive_accounts, true_positives, precision_at_k, recall_at_k, model
FROM is459_team6.sharing_files
WHERE "$path" LIKE '%/comparison.csv';
