# BQ1 results

Can a model trained on what one bank can see of its own accounts rank those accounts for review better than
rule-based monitoring? Run on 9 November 2026 on EMR Serverless (application `00ga2119lltms209`, emr-7.8.0, Spark
3.5.4, Python 3.9.25, numpy 2.0.2) over curated `ibm-aml/e2e-20261105-152512` (HI-Medium). Decision record: ADR-0005.

BQ1 is one bank's point of view. The models use only the 15 of Prepare's 17 seven-day features that a bank can
compute from its own accounts' transfers (16 inputs with the `hrs_in_to_out_missing` indicator). `cp_max_fan_in` and
`cp_max_fan_out` are left out because they count the counterparties' other transfers, which sit at other banks.
Prepare still writes them.

## Test days, K = 1,000

Each day, all eligible accounts are ranked (an account with a non-self transfer that day) and the top 1,000 are
reviewed. Positives are eligible accounts with a laundering transfer that day. Precision@1,000 = true positives /
1,000; Recall@1,000 = true positives / positives.

| Day | Eligible | Positives | Rules P@1k | Rules R@1k | Best-case rules P@1k | Best-case rules R@1k | LR P@1k | LR R@1k | GBT P@1k | GBT R@1k |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2022-09-15 | 899,565 | 3,632 | 4.9% | 1.35% | 13.1% | 3.61% | 38.3% | 10.55% | **54.5%** | **15.01%** |
| 2022-09-16 | 1,421,148 | 3,757 | 4.9% | 1.30% | 11.2% | 2.98% | 31.0% | 8.25% | **44.9%** | **11.95%** |
| True positives (15 / 16 Sep) | | | 49 / 49 | | 131 / 112 | | 383 / 310 | | 545 / 449 | |

- GBT is the selected model. It finds 11.1x (15 Sep) and 9.2x (16 Sep) as many laundering accounts as the rules
  in the same 1,000 reviews.
- Best-case rules keep the label-free thresholds but weight each rule 0-3, choosing among all 4,095 weightings by
  mean Precision@1,000 on the 13-14 Sep labels (12.9%; test days not used). Winner: `funnel` 2, `cross_bank_spread`
  1, `large_payment` 1, the other three dropped. It uses labels, so it is an upper bound for hand-tuned rules, and
  GBT still finds 4.2x / 4.0x as many (job `00ga5cvathecmg0b`, 145 s, 0.832 vCPU-h, `gold/bq1/baseline-tuned/20261109`).
- All models lose precision on 16 Sep, which has 58% more eligible accounts for about the same number of positives.
- These are simulated-data retrospective results. The score is a ranking value, not a probability.
- GIN results belong to BQ2 (bank view vs shared view), not to BQ1.

Before the change: the first models (run `t4-20261108`, pipeline run `bq1-dod-20261108`) used all 17
features, including `cp_max_fan_in` and `cp_max_fan_out`. Those two are not bank-visible, so the models were retrained
without them. With them, LR scored 38.6% / 31.1% (TP 386 / 311) and GBT 52.4% / 45.5% (TP 524 / 455) on 15 / 16 Sep.
Dropping them moved GBT by +2.1 / -0.6 points and LR by -0.3 / -0.1, so nearly all of the result comes from
bank-visible features. The rules never used them and are unchanged.

## Checks

| Check | Expected | Got |
|---|---|---|
| Eligible, 15 / 16 Sep (train, baseline and score jobs) | 899,565 / 1,421,148 | match |
| Positives, 15 / 16 Sep (train and baseline) | 3,632 / 3,757 | match |
| Test days never used to fit, tune or threshold | splits 7-12 train, 13-14 val, 15-16 test | `metrics.json` `splits`; selection on val only |
| Published queues vs the training job's test ranking | TP 545 / 449 | 545 / 449 (queues joined to labels afterwards, outside the score job) |
| Queue shape | 1,000 distinct accounts, 3 reasons each | 1,000 / 1,000 per day, all keys join to curated on the scored day (leading zeros kept) |
| Not-bank-visible features | absent from model inputs and reasons | not among the trained models' 16 inputs (`metrics.json` importances); no queue reason names `cp_max_*` on either day; unit tests cover the assembler and reasons |
| Score job reads no labels | `bq1-score` role has no label prefix | enforced by IAM (ADR-0005) |

## Model selection

Fixed before any test day was scored: within each family, then overall, the setting with the highest mean daily
Precision@1,000 on 13-14 Sep wins; ties go to the earlier grid entry. Training rows: all 20,191 positive
account-days from 7-12 Sep plus 524,053 sampled negatives (fraction 0.1, seed 1, hashed per account-day).

| Model | Setting | Val P@1k 13 Sep | Val P@1k 14 Sep | Mean |
|---|---|---:|---:|---:|
| LR | regParam 0.0 | 40.6% | 38.4% | **39.50%** |
| LR | regParam 0.01 | 32.1% | 29.8% | 30.95% |
| LR | regParam 0.1 | 32.6% | 31.0% | 31.80% |
| GBT | maxDepth 3, maxIter 50 | 43.1% | 41.7% | 42.40% |
| GBT | maxDepth 3, maxIter 100 | 45.1% | 45.3% | 45.20% |
| GBT | maxDepth 5, maxIter 50 | 56.5% | 52.0% | 54.25% |
| GBT | maxDepth 5, maxIter 100 | 58.9% | 54.0% | **56.45%** (winner) |
| Rules | 6 rules, no learning | 5.1% | 4.4% | 4.75% |

- Chosen: **GBT, maxDepth 5, maxIter 100** (runner-up LR, regParam 0.0), the same settings as before the change.
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
| ach_share | 0.271 | +0.939 |
| max_txn_per_day | 0.124 | -1.097 |
| usd_in | 0.118 | +0.300 |
| fan_out | 0.086 | +1.206 |
| active_days | 0.077 | -0.225 |
| fan_in | 0.072 | +1.121 |
| n_currencies | 0.066 | +0.398 |
| cp_banks | 0.055 | -0.252 |
| max_usd | 0.050 | +0.358 |
| usd_out | 0.032 | -0.423 |
| pass_through | 0.016 | +0.181 |
| hrs_in_to_out | 0.015 | -0.123 |
| n_pay_formats | 0.010 | +0.631 |
| hrs_in_to_out_missing | 0.005 | -0.110 |
| cross_bank_share | 0.003 | -0.000 |
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
| Prepare (skipped: `_SUCCESS` exists) | emr-job | `00ga5okmol0npo0b` | 50 s | 0.267 | 1.067 |
| train (7 fits + test scoring) | bq1-train | `00ga5omitlhsr00b` | 70 min 49 s | 37.199 | 148.796 |
| score 15 Sep | bq1-score | `00ga5pvp6u9d5g0b` | 116 s | 0.436 | 1.742 |
| score 16 Sep | bq1-score | `00ga5q1cmpnpk80b` | 134 s | 0.514 | 2.058 |
| baseline (rules, 8 Nov) | bq1-train | `00ga55hh2cepsg0b` | 132 s | 0.506 | 2.022 |

Daily scoring with a pinned model takes about 2 minutes and 0.5 vCPU-hours. Training dominates, and most of it is
the seven-fit grid. While it runs it holds the application's 32 vCPU limit, so other EMR jobs started then are
refused (four BQ2 Prepare jobs were, at 12:27-12:44). Prepare's 5 Nov run used 8.247 vCPU-hours for comparison.

## Pipeline run

Step Functions execution `bq1-bankvis-20261109`, input `{"bq1": {"retrain": true}}`, succeeded in 84 minutes:

- CheckRawZone → Prepare (skipped, 3 min) → TrainBQ1 (72 min) → ScoreBQ1, one job per test day (15 Sep 3 min,
  16 Sep 3 min) → the BQ2 GIN Score step (3 min, not part of BQ1).
- The model is `bq1/bq1-bankvis-20261109`, and `bq1_model_version` is pinned to it for daily scoring.
- Nothing was overwritten: each model and queue is named by its execution. The 17-feature models and queues from
  8 Nov are still in S3 but are superseded.

## Outputs

- Model: `s3://is459-team6-models-207567757101/bq1/bq1-bankvis-20261109/` (`model/`, `runner_up/`, `metrics.json`,
  `_SUCCESS`)
- Rules baseline: `s3://is459-team6-gold-207567757101/bq1/baseline/t4-20261108/metrics.json`
- Best-case rules: `s3://is459-team6-gold-207567757101/bq1/baseline-tuned/20261109/metrics.json`
- Queues: `s3://is459-team6-gold-207567757101/bq1/queue/bq1-bankvis-20261109/date=2022-09-15/` and
  `.../date=2022-09-16/` (`accounts.csv`, `ranking.json`, `_SUCCESS`)

Reproduce with `spark-submit bq1.py train|baseline|baseline-tuned|score ...` on the EMR application (defaults in
`train/bq1.py`), or through Step Functions with `{"bq1": {"retrain": true}}`.
