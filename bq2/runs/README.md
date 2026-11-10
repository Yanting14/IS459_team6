# BQ2 runs: bank view vs shared view

How much more laundering would a bank catch if it could see every bank's transfers? The frozen GIN
(`6e5e2da7ed914e14`) scores two views of the same days, and both views review the bank's own accounts at the
same budget:

- **bank view**: the scoring run with `SCORING_BANK=<bank>`, which keeps only transfers that the bank sends or
  receives before the graph is built.
- **shared view**: the published run without a bank filter.

`bq2.sharing compare` holds the eligible accounts, the positives and K fixed across the two views and asserts
that they match, so the only thing that differs is what the model could see.

## Results (15 and 16 Sep 2022, 16 banks)

`results/comparison.csv` follows the dashboard's contract (README, "`sharing/comparison.csv`") plus a
`model` column; `results/comparison_by_day.csv` has the same rows split by day. Both are also in
`s3://is459-team6-gold-207567757101/ibm-aml/e2e-20261105-152512/sharing/`.

| | bank view | shared view |
|---|---|---|
| true positives / reviewed | 165 / 320 | 210 / 320 |
| Precision@K | 51.6% | 65.6% |
| Recall@K (540 positive accounts) | 30.6% | 38.9% |

- The two days agree: 82 → 103 on 15 Sep and 83 → 107 on 16 Sep.
- Per bank, 12 improve, 2 tie (006, 02597) and 2 lose one true positive each (003, 012). A two-sided sign test
  gives p ≈ 0.013.

Caveats:

- K = max(10, ceil(eligible × 0.0011)) comes out at the floor of 10 for every bank on every day, so K = 20 over
  the two days. One bank's Precision@K moves 5 points per account, so read the pooled numbers and the sign
  test, not single banks.
- The model was trained on the shared view and only its inputs are filtered at scoring time. This measures
  the value of seeing more data when scoring, not of each bank training its own model.

## Bank choice

`banks_final.csv` holds the 16 banks with at least 10 positive accounts on each of 15 and 16 Sep
(`min_positives` is the smaller of the two days), ranked by `python -m bq2.sharing choose-banks`. Most banks have too few positives on a single day for a per-bank
estimate, so the experiment pools many banks over two days instead of picking a handful.

## Reproduce

Each bank-view run is one AWS Batch job using the pipeline's scoring job definition and the same environment
variables as its Score state. Submitting to Batch directly skips Prepare, which is not needed because curated
already exists, and which waits for EMR Serverless capacity that other runs may hold. The compute
environment runs one job at a time, about 25 seconds each.

```bash
cd bq2/runs
./submit_batch.sh banks_final.csv 2022-09-15 2022-09-16   # writes jobs.tsv; re-runnable
# wait until the jobs succeed (AWS Batch console, or aws batch describe-jobs)
python fetch_runs.py --jobs jobs.tsv --out downloads --manifest runs.json
cd ../..
python -m bq2.sharing compare --curated <local curated prefix> --manifest bq2/runs/runs.json \
    --ratio 0.0011 --floor 10 --model gin --out-dir bq2/runs/sharing
```

- `fetch_runs.py` reads each run's gold receipt, checks its status and options, and downloads its
  `transfers.parquet`. Each day's shared run is the execution `bq2-shared-<day>`.
- `--curated` needs `transfers/` and `transfer_labels/` for the days compared.
- `results/jobs.tsv` lists the Batch job ids of the runs behind `results/`.

## In the pipeline

The steps above now also run inside the Step Functions pipeline, with the same execution ids, banks, days,
ratio and floor (`infra/bq2.tf`): `BQ2Score` scores every view on Batch, then `BQ2Compare` runs `bq2/cloud.py`,
which checks every receipt and each scores file's sha256 and publishes `comparison.csv` and
`comparison_by_day.csv` to `<gold>/<curated_prefix>/sharing/`. It is off until the scoring image is rebuilt to
contain `bq2/` (`scripts/Publish-Image.ps1 scoring <tag>`, then set `scoring_image_digest`) and `enable_bq2` is set
to `true`. Scoring reuses a completed run with identical inputs, so the runs behind `results/` are not repeated.
The manual steps above stay as the way to reproduce or debug one run.
