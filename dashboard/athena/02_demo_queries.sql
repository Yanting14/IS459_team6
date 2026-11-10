-- Demo queries for the team's Athena layer.
-- Run:   python athena\run_sql.py athena\02_demo_queries.sql
-- Each one reproduces a number the dashboard or the docs already show, so the two can be checked against each other.
-- Names are not qualified: the runner sets the database to is459_team6.

-- 1. Transfers per day, 1-16 Sep. Shows partition pruning: only these 16 folders are read.
--    Expect about 1.93M on a weekday and 16 Sep = 3,021,866 (README, "The HI-Medium run").
SELECT "date", count(*) AS transfers
FROM transfers
WHERE "date" BETWEEN '2022-09-01' AND '2022-09-16'
GROUP BY "date"
ORDER BY "date";

-- 2. How many laundering transfers cross banks? (Sharing page, section 1.) Expect close to 99% cross-bank.
SELECT t.sender_bank_id <> t.receiver_bank_id AS crosses_banks,
       count(*)                               AS laundering_transfers,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
FROM transfers t
JOIN transfer_labels l
  ON t.transfer_id = l.transfer_id AND t."date" = l."date"
WHERE l.laundering_mark = 1
  AND t."date" BETWEEN '2022-09-01' AND '2022-09-30'
  AND l."date" BETWEEN '2022-09-01' AND '2022-09-30'
GROUP BY 1
ORDER BY 1;

-- 3. Laundering rings by pattern type.
SELECT pattern_type, count(DISTINCT ring_id) AS rings, count(*) AS transfers
FROM rings
GROUP BY pattern_type
ORDER BY rings DESC;

-- 4. How much of a ring can the best-placed single bank see? (Sharing page, section 1; same definition as the dashboard.)
--    A bank "sees" a transfer when it is the sender's or the receiver's bank.
--    Expect on the real data: 2,756 rings, about 42.5% not fully visible to any bank, best bank sees about 71% on average.
WITH r AS (
  SELECT ring_id, pattern_type, sender_bank_id AS s, receiver_bank_id AS d,
         row_number() OVER () AS rid
  FROM rings
),
seen AS (                                   -- one row per (transfer, bank that sees it)
  SELECT DISTINCT ring_id, pattern_type, rid, bank
  FROM r CROSS JOIN UNNEST(ARRAY[s, d]) AS u(bank)
),
per_bank AS (
  SELECT ring_id, pattern_type, bank, count(*) AS seen FROM seen GROUP BY ring_id, pattern_type, bank
),
best AS (
  SELECT ring_id, pattern_type, max(seen) AS seen FROM per_bank GROUP BY ring_id, pattern_type
),
totals AS (
  SELECT ring_id, count(*) AS transfers FROM r GROUP BY ring_id
)
SELECT count(*)                                                        AS rings,
       round(100.0 * avg(CASE WHEN b.seen < t.transfers THEN 1.0 ELSE 0.0 END), 1) AS pct_no_single_bank_sees_all,
       round(100.0 * avg(1.0 * b.seen / t.transfers), 0)               AS avg_pct_seen_by_best_bank
FROM best b
JOIN totals t ON b.ring_id = t.ring_id;
