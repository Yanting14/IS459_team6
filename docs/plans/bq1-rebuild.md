# Plan: BQ1 on EMR, GIN moved to BQ2

Owner: Toby (BQ1). Interfaces with Jun Jie (dashboard), person 4 (BQ2), Jun Yu (prepare/infra), Jesmond (GIN).
Written 8 Nov 2026 against `main` at `d23ac53` and branch `dashboard_JJ` at `353d08f`.

## Decisions this plan implements

1. **BQ1 model** = Spark ML trained on EMR Serverless on the seven-day account-day features already in curated.
   Logistic regression and gradient-boosted trees are both trained; the winner is chosen by a rule fixed in advance
   (validation Precision@1,000). Training is a visible, re-runnable pipeline step that writes a new model version.
2. **BQ1 baseline** = hand-written AML rules on the same features, no learning. It stands in for banks' current
   rule-based monitoring, which the proposal says any model must beat.
3. **The frozen GIN (Jesmond) is not BQ1's answer.** It is reused unchanged for BQ2 (bank view vs shared view) and
   shown in BQ1 only as a reference column. Do not retrain, edit or move it. `scoring/` and `scoring/vendor/` are
   out of scope.
4. **No drift-triggered retraining.** Retraining = run the Train step on demand or after a new ingest. Scoring always
   uses one pinned model version.
5. **Honest test days.** Report test results on 15 and 16 Sep 2022 only. 14 Sep likely falls inside the GIN's
   validation period, so it is not a fair GIN test day.

## Context Claude Code needs

- Read first: `README.md`, `GLOSSARY.md`, `docs/adr/0001`-`0004`, `prepare/prepare.py`, `prepare/test_prepare.py`,
  `infra/emr.tf`, `infra/stepfunctions.tf`, `infra/scoring.tf`, `docs/scoring-validation.md`.
- Vocabulary (glossary): an **account** is `(bank_id, account_number)`, never the number alone. **Bank view** = only
  transfers where that bank is sender's or receiver's bank. **Shared view** = all transfers. **Label** is never a
  model input.
- Curated layout (version prefix e.g. `ibm-aml/v2` or `ibm-aml/e2e-20261105-152512`):
  - `features/date=YYYY-MM-DD/` keyed by `bank_id, account_number, date`. Columns are `prepare.FEATURES`:
    `fan_in, fan_out, cp_banks, cross_bank_share, cp_max_fan_in, cp_max_fan_out, usd_in, usd_out, max_usd,
    pass_through, hrs_in_to_out, max_txn_per_day, active_days, n_pay_formats, ach_share, n_currencies,
    history_days` (16 proposal features + `history_days`). Nulls are left in on purpose (filling belongs to training).
  - `labels/date=YYYY-MM-DD/`: same keys + `laundered_24h` (0/1), exactly the features' account-days.
  - Feature days are 1-16 Sep 2022. One row = an account with at least one non-self transfer that day. This is the
    same eligibility the GIN daily ranking uses.
- Infra conventions: everything in Terraform (`infra/`), shared state in S3. EMR Serverless app
  `is459-team6-spark` (EMR 7, Spark 3.5, 32 vCPU cap) is reused. Job code is uploaded by Terraform as an
  `aws_s3_object` (see `prepare_job` in `infra/emr.tf`). Step Functions runs
  `CheckRaw -> Ingest -> Prepare -> Score`. Buckets: raw, curated, gold, models (ADR-0003).
- Repo conventions: an output prefix that already exists is refused; `_SUCCESS` is written last; reruns use a new
  prefix. Conventional commits (`feat(train): ...`). Code style: plain, few comments, no new dependencies beyond
  what EMR 7 ships (pyspark, numpy). Local Spark tests with pytest; on a too-new JDK use `prepare/Dockerfile.test`.

## Tasks

### T1. `train/bq1.py`: one Spark script, three modes

`spark-submit train/bq1.py {train|score|baseline} ...`. Only the entry-point script is uploaded to EMR, so
`import prepare` will fail there. Copy `FEATURES` and `LABEL` into `bq1.py` and add a test asserting they equal
`prepare`'s. Do not modify `prepare.py`.

**train** `--curated <prefix> --out s3://<models>/bq1/<version> [--train-from 2022-09-07 --train-to 2022-09-12
--val 2022-09-13,2022-09-14 --test 2022-09-15,2022-09-16 --k 1000 --neg-fraction 0.1 --seed 1]`
1. Join features to labels on `(bank_id, account_number, date)`. Assert the join is 1:1 and that no label column
   is in the feature vector.
2. Split by date only. Days 1-6 are excluded by default (incomplete lookback windows).
3. Train set only: keep every positive and a seeded `--neg-fraction` sample of negatives. Validation and test are
   never sampled.
4. Preprocessing in a Spark ML `Pipeline`:
   - add an `hrs_in_to_out_missing` indicator
   - impute nulls with the training median
   - LR only: `log1p` on counts and USD amounts, then `StandardScaler`
5. Grid, about 7 fits:
   - LR `regParam` in {0.0, 0.01, 0.1}
   - GBT `maxDepth` in {3, 5} x `maxIter` in {50, 100}
6. Select each model's best setting by mean daily Precision@K on the validation days. Pick the overall winner by
   the same metric. Write the rule into the output so it is visibly fixed in advance.
7. Score test days with the winner and the runner-up. Daily ranking per day: sort by the model's positive-class score (a ranking value, not a probability), ties
   broken by `(bank_id, account_number)`. Take the top K.
8. Write to `--out`:
   - `model/` (the winning `PipelineModel`)
   - `runner_up/`
   - `metrics.json`: per model, per split and day: eligible, positives, TP, Precision@K, Recall@K, plus grid
     results and the chosen setting
   - `_SUCCESS` last; refuse an existing `--out`

**baseline** `--curated <prefix> --out s3://<gold>/bq1/baseline/<version> [same date args]`
1. Thresholds are percentiles of the **training days only** (label-free). Suggested rules:

   | Rule | Condition |
   |---|---|
   | Rapid pass-through | `pass_through >= p99` and `hrs_in_to_out <= p10` |
   | Funnel | `fan_in >= p99` |
   | Fan-out | `fan_out >= p99` |
   | Bursts | `max_txn_per_day >= p99` |
   | Cross-bank spread | `cp_banks >= p99` |
   | Large payment | `max_usd >= p99` |

2. Score = number of rules fired. Tie-break by `usd_out` descending, then the account key.
3. Evaluate on validation and test days with the same K and eligibility. Write `metrics.json` with the
   thresholds, then `_SUCCESS`.

**score** `--curated <prefix> --model s3://<models>/bq1/<version> --date 2022-09-15 --k 1000
--out s3://<gold>/bq1/queue/<run>`
1. Load the pinned model and score that day's account-days.
2. Write `accounts.csv` with columns `rank, bank_id, account_id, score, model_version, window_start, cutoff,
   reason_1, reason_2, reason_3`.
   - `account_id` = `account_number`, kept as a string with any leading zeros. This keeps the GIN `accounts.csv`
     column names the dashboard already reads.
   - `window_start` = date. `cutoff` = date + 1 day.
3. Reasons: for each account, rank each feature's value against that day's eligible population (percentile). The
   top 3 become human-readable strings, e.g. `fan_in in top 0.1% (412)`. These are descriptive flags, not model
   attributions; say so in `ranking.json`.
   - Each feature needs a direction in a fixed map: **high is suspicious**, **low is suspicious**
     (`hrs_in_to_out`), or **never a reason** (`history_days`, `active_days`). Without the map, reasons will name
     ordinary accounts' features.
   - `score` is a ranking value, not a probability: negative sampling skews it. Never label it a probability.
4. Also write `ranking.json` (k, eligible, model version, how reasons are made) and `_SUCCESS`.
5. No labels are read in this mode.

### T2. Tests: `train/test_bq1.py`, local Spark, tiny hand-made curated zone

- The split never puts a date in two sets. Negatives are sampled in train only. The seed is deterministic.
- No column named like a label reaches the assembler.
- The Precision@K / Recall@K helper is correct on a hand-checked example, including fewer than K eligible.
- Rule thresholds use training days only. Changing a test-day value cannot change a threshold.
- `score` refuses an existing output, writes `_SUCCESS` last, preserves leading zeros, and gives 3 reasons per
  account.
- `train` end-to-end on the toy data produces `metrics.json` with every grid entry.
- The copied `FEATURES` and `LABEL` equal `prepare.FEATURES` and `prepare.LABEL`.
- The reason direction map covers every feature. A low `hrs_in_to_out` can be a reason; `history_days` and
  `active_days` never are.

### T3. Infra (coordinate with Jun Yu before applying)

- Upload `train/bq1.py` as an `aws_s3_object` next to `prepare.py`.
- Two roles, because labels live under the same curated prefix the current `emr-job` role can read
  (`infra/emr.tf`). An ADR note alone cannot stop `score` reading them.
  - **`bq1-train` role:** read curated `<prefix>/features/*` and `<prefix>/labels/*`, write `models/bq1/*` and
    `gold/bq1/baseline/*`. Used by `train` and `baseline`.
  - **`bq1-score` role:** read curated `<prefix>/features/*` and `models/bq1/*`, write `gold/bq1/queue/*`. No access
    to `labels/` or `transfer_labels/`.
  - Both need the job script object and their log prefix. Neither gets write access to curated.
- Step Functions order: `Prepare` -> `Choice` on `$.bq1.retrain` (default false) -> `ScoreBQ1` -> the existing
  `ScoringConfiguration` (GIN). This keeps the GIN path untouched.
  - **retrain = true:** `TrainBQ1` (EMR Serverless sync job) -> `ScoreBQ1`, and `ScoreBQ1` uses the model that run
    just wrote, not `bq1_model_version`.
  - **retrain = false:** `ScoreBQ1` uses `bq1_model_version` (Terraform variable, overridable in execution input).
  - Output names must be unique because `--out` refuses existing paths. Use the execution name:
    `models/bq1/<execution>/` and `gold/bq1/queue/<execution>/`.
  - `ScoreBQ1` needs a date input, e.g. `$.bq1.date` (default `2022-09-15`).
- Keep the existing GIN `Score` state as is (`enable_scoring` default stays false). It now serves BQ2.
- Add a mocked test in `infra/tests/` for the new states. Run `terraform validate` and `fmt`.

### T4. Run and record (real AWS, curated prefix with `_SUCCESS`)

0. **Confirm the data first.** Training needs a curated prefix holding `features/`, `labels/` and `_SUCCESS`, not
   just the raw zone. The 5 Nov run's prefix `ibm-aml/e2e-20261105-152512` was built after the last `prepare.py`
   change (4 Nov), so it should qualify. Check with Jun Yu that this is the prefix to use. Note that the existing
   role's access follows the `curated_prefix` Terraform variable (default `ibm-aml/v2`).
1. `train` once. `baseline` once. `score` for 15 and 16 Sep.
2. **Validation checks before trusting any number. All four must match exactly; any gap is a bug.**
   - Eligible account-days: **899,565** on 15 Sep, **1,421,148** on 16 Sep (the GIN's daily eligible counts in
     `docs/scoring-validation.md`).
   - Positives: **3,632** on 15 Sep, **3,757** on 16 Sep. `build_labels` counts laundering self-transfers and uses
     the same eligibility as the GIN, so they must be equal.
   - Test metrics must come from the test days only. The model must never have seen 15-16 Sep.
3. Write `docs/bq1-results.md` with one table: rules vs LR vs GBT vs GIN (reference, from
   `scoring-validation.md`), Precision@1,000 and Recall@1,000 on 15 and 16 Sep. Add the chosen hyperparameters, the
   runtime, the EMR usage and the global feature importances.

### T5. Docs

- Add `docs/adr/0005-bq1-trained-on-emr.md`. It records:
  - why BQ1 uses the seven-day features on EMR
  - that the GIN moves to BQ2
  - the test-day rule
  - that reasons are descriptive
  - that labels are reachable only by the train role
  - that 7-day windows on 15-16 Sep include training-day transfers. This is not leakage: features carry no account
    identity and labels are per day.
- Add a line to ADR-0004 pointing to it.
- Add `docs/deviations.md` mirroring the team's deviation log.

## Interfaces owned by others (do not build here, but check the contract)

**Dashboard (Jun Jie, branch `dashboard_JJ`).** Validated 8 Nov. It is well built and its 11 tests pass on the
sample data. It has never run on real output. Changes needed for this plan:

- **Queue page:**
  - Point it at the BQ1 queue (`gold/bq1/queue/<run>/`) instead of the GIN's.
  - Add a reasons column.
  - Add a "model vs rules vs GIN" panel reading `metrics.json` from T1/T4.
  - Delete the hard-coded `DOCUMENTED` GIN numbers, which include 14 Sep.
  - Replace "frozen graph neural network, not retrained" with the BQ1 model description.
- **Investigation page:**
  - `supporting_transfer_id` and `transfers.parquet` risk scores are GIN-only. With the BQ1 queue, show the
    account's own transfers without per-transfer scores.
  - Verify the queue's `account_id`/`bank_id` join to curated `account_number`/`bank_id` on real data (leading
    zeros).
- **Sharing page:** add a `model` column to `sharing/comparison.csv` so BQ2 can show GIN and, optionally, the BQ1
  model side by side.
- **Data access:** the dashboard reads S3/local Parquet through DuckDB, not Athena + Glue as proposed. Either add
  Glue tables and an Athena workgroup over gold (person 1/6), or log it as a deviation.

**BQ2 (person 4).** Use the GIN as is:
- `prepare --bank B` + `rank --bank B` vs shared `prepare` + `rank --bank B`, on 15-16 Sep only.
- One shared run per day is reused across banks.
- Pick 5-10 banks with enough positives.
- Set K per bank (e.g. a fixed share of its eligible accounts), not 1,000.
- Write `comparison.csv` in the dashboard's contract plus `model`.
- Optional: the BQ1 model on bank-view features from `prepare.build_features` (only `cp_max_fan_in` and
  `cp_max_fan_out` change between views, so expect a small uplift).

**Jesmond.** Commit the GIN training code and the exact train/validation/test split dates. BQ2's headline depends
on that model.

## Definition of done

- `pytest train` and `pytest prepare` pass. `terraform validate` passes.
- One Step Functions execution with `bq1.retrain = true` goes Prepare -> TrainBQ1 -> ScoreBQ1 and publishes a
  queue with 1,000 accounts and 3 reasons each.
- `docs/bq1-results.md` exists with the four-way comparison on 15-16 Sep and passes the T4 checks.
- No label is read by `score`, enforced by the `bq1-score` role having no access to label prefixes. No GIN file
  changed.
