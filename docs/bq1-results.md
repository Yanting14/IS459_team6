# BQ1 results

Can a model trained on the seven-day account-day features rank accounts for review better than rule-based
monitoring? Run on 8 November 2026 on EMR Serverless (application `00ga2119lltms209`, emr-7.8.0, Spark 3.5.4,
Python 3.9.25, numpy 2.0.2) over curated `ibm-aml/e2e-20261105-152512` (HI-Medium). Decision record: ADR-0005.

## Test days, K = 1,000

Each day, all eligible accounts are ranked (an account with a non-self transfer that day) and the top 1,000 are
reviewed. Positives are eligible accounts with a laundering transfer that day. Precision@1,000 = true positives /
1,000; Recall@1,000 = true positives / positives.

| Day | Eligible | Positives | Rules P@1k | Rules R@1k | LR P@1k | LR R@1k | GBT P@1k | GBT R@1k | GIN P@1k (ref) | GIN R@1k (ref) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2022-09-15 | 899,565 | 3,632 | 4.9% | 1.35% | 38.6% | 10.63% | **52.4%** | **14.43%** | 80.70% | 22.22% |
| 2022-09-16 | 1,421,148 | 3,757 | 4.9% | 1.30% | 31.1% | 8.28% | **45.5%** | **12.11%** | 72.30% | 19.24% |
| True positives (15 / 16 Sep) | | | 49 / 49 | | 386 / 311 | | 524 / 455 | | 807 / 723 | |
| Best-case rules, tuned on 13-14 Sep labels (15 / 16 Sep; TP 131 / 112) | | | 13.1% / 11.2% | 3.61% / 2.98% | | | | | | |

- GBT is the selected model. It finds 10.7x (15 Sep) and 9.3x (16 Sep) as many laundering accounts as the rules
  in the same 1,000 reviews.
- Best-case rules keep the label-free thresholds but weight each rule 0-3, choosing among all 4,095 weightings by
  mean Precision@1,000 on the 13-14 Sep labels (12.9%; test days not used). Winner: `funnel` 2, `cross_bank_spread`
  1, `large_payment` 1, the other three dropped. It uses labels, so it is an upper bound for hand-tuned rules, and
  GBT still finds 4.0x / 4.1x as many (job `00ga5cvathecmg0b`, 145 s, 0.832 vCPU-h, `gold/bq1/baseline-tuned/20261109`).
- The frozen GIN is a reference, not a competitor. Its numbers come from [scoring validation](scoring-validation.md),
  on the same days, eligibility, positives and K. It sees the transfer graph and cumulative history, which the
  seven-day features do not, and it was trained elsewhere on splits not in this repository (ADR-0004).
- All models lose precision on 16 Sep, which has 58% more eligible accounts for about the same number of positives.
- These are simulated-data retrospective results. The score is a ranking value, not a probability.

## Checks

| Check | Expected | Got |
|---|---|---|
| Eligible, 15 / 16 Sep (train, baseline and score jobs) | 899,565 / 1,421,148 | match |
| Positives, 15 / 16 Sep (train and baseline) | 3,632 / 3,757 | match |
| Validation day 14 Sep vs the GIN's table | 900,182 eligible / 3,524 positives | match |
| Test days never used to fit, tune or threshold | splits 7-12 train, 13-14 val, 15-16 test | `metrics.json` `splits`; selection on val only |
| Published queues vs the training job's test ranking | TP 524 / 455 | 524 / 455 (queues joined to labels afterwards, outside the score job) |
| Queue shape | 1,000 distinct accounts, 3 reasons each | 1,000 / 1,000, no empty reason, all keys join to curated (leading zeros kept) |
| Score job reads no labels | `bq1-score` role has no label prefix | enforced by IAM (ADR-0005) |

## Model selection

Fixed before any test day was scored: within each family, then overall, the setting with the highest mean daily
Precision@1,000 on 13-14 Sep wins; ties go to the earlier grid entry. Training rows: all 20,191 positive
account-days from 7-12 Sep plus 524,053 sampled negatives (fraction 0.1, seed 1, hashed per account-day).

| Model | Setting | Val P@1k 13 Sep | Val P@1k 14 Sep | Mean |
|---|---|---:|---:|---:|
| LR | regParam 0.0 | 41.6% | 37.2% | **39.40%** |
| LR | regParam 0.01 | 32.4% | 29.8% | 31.10% |
| LR | regParam 0.1 | 33.4% | 31.7% | 32.55% |
| GBT | maxDepth 3, maxIter 50 | 45.1% | 43.0% | 44.05% |
| GBT | maxDepth 3, maxIter 100 | 48.8% | 46.2% | 47.50% |
| GBT | maxDepth 5, maxIter 50 | 53.5% | 50.9% | 52.20% |
| GBT | maxDepth 5, maxIter 100 | 55.6% | 53.6% | **54.60%** (winner) |
| Rules | 6 rules, no learning | 5.1% | 4.4% | 4.75% |

- Chosen: **GBT, maxDepth 5, maxIter 100** (runner-up LR, regParam 0.0).
- The largest GBT setting won, so a deeper or longer grid might do better. That was not tried, to keep the grid
  fixed in advance.
- Preprocessing (in the saved `PipelineModel`): an `hrs_in_to_out_missing` indicator, median imputation from
  training rows, and for LR only `log1p` on counts and USD amounts, then standardisation.

Rule thresholds (training-day percentiles, label-free): `pass_through` p99 0.9151 with `hrs_in_to_out` p10 4.72 h,
`fan_in` p99 6, `fan_out` p99 10, `max_txn_per_day` p99 19, `cp_banks` p99 11, `max_usd` p99 16,338,231.84. Score
= rules fired, ties broken by `usd_out`.

## Global feature importances

GBT: Spark's `featureImportances` (sums to 1). LR: coefficients on standardised inputs (sign = direction).

| Feature | GBT | LR coefficient |
|---|---:|---:|
| ach_share | 0.256 | +1.008 |
| max_txn_per_day | 0.102 | -1.222 |
| usd_in | 0.096 | +0.294 |
| active_days | 0.074 | -0.256 |
| max_usd | 0.068 | +0.362 |
| fan_in | 0.068 | +1.139 |
| fan_out | 0.060 | +1.301 |
| n_currencies | 0.059 | +0.398 |
| cp_max_fan_in | 0.058 | -0.145 |
| cp_max_fan_out | 0.054 | +0.281 |
| cp_banks | 0.038 | -0.198 |
| usd_out | 0.033 | -0.434 |
| pass_through | 0.013 | +0.182 |
| hrs_in_to_out | 0.011 | -0.090 |
| n_pay_formats | 0.004 | +0.657 |
| hrs_in_to_out_missing | 0.004 | -0.070 |
| cross_bank_share | 0.003 | -0.008 |
| history_days | 0.000 | 0.000 |

- `history_days` carries nothing. It counts window days since the dataset's first day, so it is 7 for every
  account from 7 Sep onwards.
- LR coefficients of correlated features (`fan_in`, `fan_out`, `max_txn_per_day`) offset each other. Read them as
  a set, not one by one.
- Importances are global. Each queued account's three reasons are separate descriptive flags (the features where
  it is most extreme that day), not attributions of the model's score.

## Runtime and EMR usage

EMR billed usage, not a cost estimate. Times are job execution time from EMR.

| Job | Role | EMR job id | Execution time | vCPU-hours | Memory GB-hours |
|---|---|---|---:|---:|---:|
| Smoke test (versions) | bq1-train | `00ga55fddlp0d80b` | 53 s | 0.267 | 1.067 |
| baseline | bq1-train | `00ga55hh2cepsg0b` | 132 s | 0.506 | 2.022 |
| train (7 fits + test scoring) | bq1-train | `00ga55hgok9tuo0b` | 72 min 13 s | 37.612 | 150.449 |
| score 15 Sep | bq1-score | `00ga56rsnf79fg0b` | 118 s | 0.449 | 1.796 |
| score 16 Sep | bq1-score | `00ga56rt0rbc9o0b` | 142 s | 0.556 | 2.222 |

Daily scoring with a pinned model takes about 2 minutes and 0.5 vCPU-hours. Training dominates, and most of it is
the seven-fit grid. Prepare's 5 Nov run used 8.247 vCPU-hours for comparison.

## Pipeline run

Step Functions execution `bq1-dod-20261108`, input `{"bq1": {"retrain": true}}`, succeeded in 79 minutes:

- CheckRawZone → Prepare (skipped: its `_SUCCESS` exists, 3 min) → TrainBQ1 (70 min) → ScoreBQ1 (15 Sep, 3 min)
  → GIN Score (2.5 min).
- The retrained model is `bq1/bq1-dod-20261108`. It picked the same setting (GBT, maxDepth 5, maxIter 100) with the
  same test TP (524 / 455).
- Its queues hold the same 1,000 accounts as the T4 queues on both days: 15 Sep from the pipeline, 16 Sep scored
  by hand with the same model.
- Scores differ by up to 0.0016, so a few ranks swap from rank 4 onwards. Spark's tree fitting on EMR is not
  bit-for-bit repeatable, but it was repeatable at the level that matters here.
- The GIN step matched its input identity to the 5 Nov run (`run=713d70d6…`). It reused those 14 Sep predictions
  without re-scoring and wrote only an execution receipt.
- Nothing was overwritten: each model and queue is named by its execution.

## Outputs

- Model: `s3://is459-team6-models-207567757101/bq1/t4-20261108/` (`model/`, `runner_up/`, `metrics.json`,
  `_SUCCESS`)
- Baseline: `s3://is459-team6-gold-207567757101/bq1/baseline/t4-20261108/metrics.json`
- Queues: `s3://is459-team6-gold-207567757101/bq1/queue/t4-20261108-0915/` and `.../t4-20261108-0916/`
  (`accounts.csv`, `ranking.json`, `_SUCCESS`)
- Pipeline run: model `s3://is459-team6-models-207567757101/bq1/bq1-dod-20261108/`, queues
  `s3://is459-team6-gold-207567757101/bq1/queue/bq1-dod-20261108/` (15 Sep) and `.../bq1-dod-20261108-0916/`
  (16 Sep, scored by hand)

Reproduce with `spark-submit bq1.py train|baseline|score ...` on the EMR application (arguments as above, defaults
in `train/bq1.py`), or through Step Functions with `{"bq1": {"retrain": true}}`.
