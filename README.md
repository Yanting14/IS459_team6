# IS459_team6

AML detection pipeline on AWS (us-east-1), built with Terraform.

## Layout

```
infra/                 Terraform (S3 raw + curated zones, ingest task, EMR Serverless,
                       Step Functions, schedule; state in S3)
ingest/                Fargate container: Kaggle -> S3 raw zone
prepare/               Spark job (EMR Serverless): raw zone -> curated zone
lambdas/check_raw/     Lambda: has the raw zone already been ingested?
scoring/              Frozen GIN graph preparation, inference, ranking and evaluation
scripts/               Helper scripts
```

## Ingestion flow

```
EventBridge (daily, off by default) / manual start
  -> Step Functions
     -> CheckRawZone (Lambda)      does s3://<raw>/ibm-aml/v1/_SUCCESS exist?
     -> yes: skip to Prepare
     -> no:  Ingest (ECS Fargate)  download from Kaggle, unzip, upload to S3,
                                   write manifest.json, then _SUCCESS
     -> Prepare (EMR Serverless)   raw zone -> curated zone, waits for the job
     -> Score (optional, Batch)    stage graph/model, score, rank, publish gold
```

The dataset is static, so ingestion really runs once. Daily runs skip it. If the
task fails partway, a re-run skips files already in S3 and carries on.

Prepare runs on a shared EMR Serverless Spark application (`is459-team6-spark`,
EMR 7, at most 32 vCPU and 128 GB, auto-stopping after 15 minutes idle) that
Training and Analyse will reuse. Terraform uploads `prepare/prepare.py` to
`s3://<curated>/code/prepare.py`, and the state machine starts it synchronously with
a 1-hour timeout, retrying only service-level transients: a job that failed on its
own data would spend another hour reaching the same verdict. Spark driver and
executor logs land under `s3://<curated>/logs/`.

The job runs as `is459-team6-emr-job`, which can read the raw prefix and the job
code and read and write the curated prefix. It has no write access to the raw
bucket.

Raw zone layout:

```
s3://is459-team6-raw-<account>/ibm-aml/v1/
  transactions/variant=HI/size=medium/HI-Medium_Trans.csv
  transactions/variant=HI/size=large/HI-Large_Trans.csv
  accounts/variant=HI/size=.../HI-..._accounts.csv
  patterns/variant=HI/size=.../HI-..._Patterns.txt
  manifest.json          sizes, line counts, sha256 for each file
  _SUCCESS
```

## Prepare job

`prepare/prepare.py` reads the raw transfer and patterns files for one dataset size, sets
aside the rows whose data is broken, and writes clean transfers, account-day features,
labels and rings to the curated zone, then a quality report, then `_SUCCESS`:

```
<curated>/<version-prefix>/
  transfers/date=YYYY-MM-DD/*.parquet    sorted by sender_bank_id
  features/date=YYYY-MM-DD/*.parquet     one row per account-day, sorted by bank_id
  labels/date=YYYY-MM-DD/*.parquet       laundered_24h, at the features' account-day grain
  transfer_labels/date=YYYY-MM-DD/*.parquet  individual transfer ground truth, keyed by transfer_id
  rings/*.parquet                        one row per transfer listed in the patterns file
  quarantine/run_id=<run_id>/*.parquet   rejected rows, with reason, source file and row number
  quality/<run_id>.json                  row counts and reasons, USD rates, self-transfers,
                                         the days and banks seen, the days featurised
  _SUCCESS
```

A row is quarantined when a field is missing or blank, the timestamp cannot be parsed,
an amount is not a positive number, or the currency or payment format is not in the
dataset's known set. Being a self-transfer is not a reason on its own. If the
quarantined share exceeds `--quarantine-threshold` (default `0.01`), the job writes the
quality report and then fails, so the evidence survives the failure.

`build_features` turns clean transfers into one row per account-day: the account made
or received at least one non-self transfer that day, within 1 to 16 Sep, with every
feature measured over the 7 days ending on that day. It is a pure function and the
contract with the Analyse step (ADR-0002), which calls it on transfers filtered to one
bank to get that bank's view of the same features, so it must depend on nothing but the
transfers it is given -- `history_days` therefore counts window days from 1 Sep rather
than from the first day present in the data. A missing feature value stays null; filling
it in belongs to Training's preprocessing.

Labels and rings are the label side of the curated zone (ADR-0002): Training joins them
deliberately, and the features table never carries them. `laundered_24h` is 1 when the
account sent or received a transfer marked as laundering that day, at exactly the account
features' grain -- the account-days come from the features themselves, so a day with no
laundering transfer is a 0 rather than a missing row, and a laundering self-transfer can
never add an account-day of its own. Rings parse the patterns file, whose blocks of
transfer rows sit between `BEGIN` and `END LAUNDERING ATTEMPT` lines naming each ring's
shape. A ring's id is its block's position in the file, since the file names none, and the
pattern type is the shape without the size parameter the file appends to most of them
(`CYCLE`, not `CYCLE:  Max 10 hops`).

Quarantine is partitioned by run, not by date as ADR-0001 asks of the curated tables:
it is audit output rather than something to read a day at a time, and a row with an
unparseable timestamp has no date to partition on.

If `_SUCCESS` already exists under the version prefix, the job does nothing, so
bumping `curated_prefix` is what forces a fresh build.

### The HI-Medium run

The first real run, 4 Nov 2026, through the pipeline on the shared EMR Serverless
application, at its 32 vCPU and 128 GB cap:

```text
31,898,238 rows in, 0 quarantined       the 1% gate never came near failing
122,394 banks                           day-only partitioning (ADR-0001) is the right call
1 to 28 Sep 2022                        but 99.98% of rows fall in the first 16 days
2,561,860 self-transfers (8.03%)        72 of them marked as laundering
15 currencies, every one priced in USD
24m41s of job time, 26m31s end to end
11.019 vCPU-hours, 44.076 GB-hours      US$0.83
```

A second execution over the same version prefix finds `_SUCCESS` and returns before
reading anything: 52 seconds of job time, US$0.02, and not one object in the curated
zone rewritten. That is what makes a daily schedule affordable on a static dataset.

The transfers run to 28 Sep while the features stop at 16, and the row counts show why
the spec chose 16. The dataset is generated over Thu 1 to Fri 16 Sep with a weekly
rhythm -- around 1.93M rows on a weekday, 830K at a weekend, 2.6 to 3.0M on a Friday --
and then stops. What follows is a decaying tail of stragglers:

```text
16 Sep  3,021,866     Friday, the last full day
17 Sep      2,020     Saturday
18 Sep      1,591
...
28 Sep          7
```

This is the dataset's own design, not a surprise: the data card calls Sep 1-16 HI-Medium's
"primary period", and its author explains the tail as laundering patterns that take several
days to play out and so run past the day the simulation stops.

All twelve tail days together hold 6,987 rows, 0.022% of the file, so the window loses
no meaningful volume. It does lose signal out of proportion to its size: 58.5% of those
rows carry the laundering mark against 0.098% in the main period, which is 4,084 of the
dataset's 35,230 laundering transfers, or 11.6%. Those account-days would have nearly
empty 7-day lookback windows, so they are poor training rows, but the trade is a real
one and is recorded on #2 rather than decided here.

Which days those are follows `--size`, since each size has its own primary period: 1-10
Sep for small, 1-16 Sep for medium, 1 Aug to 5 Nov for large, the same for HI and LI. The
quality report gives the period it used as `primary_period`, beside the `first_day` and
`last_day` the input held, and an unknown size is refused before anything is read.

`--input` and `--output` take S3 URIs or local folders, which is how the same script
runs on EMR Serverless and under the local tests:

```bash
spark-submit prepare/prepare.py --input s3://<raw>/ibm-aml/v1   --output s3://<curated> --size medium --version-prefix ibm-aml/v2 --quarantine-threshold 0.01
```

Tests run the job on a small hand-made raw zone with a local Spark session.
They need Python 3.11 and a JDK (17 or 21, with `javac` on the PATH):

```bash
pip install -r prepare/requirements-dev.txt
python -m pytest prepare
```

Spark 3.5 does not start at all on JDK 27, which took away both `Subject.getSubject` and
the `-Djava.security.manager=allow` option it falls back on. Where the machine's JDK is
too new, run the suite in a container holding JDK 17 instead:

```bash
docker build -f prepare/Dockerfile.test -t is459-tests prepare
docker run --rm -v "$PWD:/repo" -w /repo/prepare is459-tests python -m pytest -q
```

On Windows no winutils or `HADOOP_HOME` is needed: `prepare/conftest.py` compiles
`WindowsLocalFileSystem.java`, a test-only stand-in for Hadoop's local filesystem,
which otherwise needs native Hadoop binaries there.

## Terraform state

State lives in `s3://is459-team6-tfstate-207567757101/infra/terraform.tfstate`, so
everyone plans against the same reality. `use_lockfile = true` has S3 itself hold the
lock, which keeps two applies from overlapping, and the bucket is versioned, so a
clobbered state can be rolled back.

`terraform init` picks all of this up from the backend block in `infra/providers.tf`.
If you still have a local `infra/terraform.tfstate` from before the backend existed,
hand it over instead of starting empty:

```bash
terraform -chdir=infra init -migrate-state
```

A bucket cannot be managed by the configuration whose state it holds, so this one was
created once by hand, with `create-bucket`, `put-bucket-versioning` (enabled),
`put-bucket-encryption` (AES256), `put-public-access-block` (all four on) and a
TLS-only `put-bucket-policy` of the same shape as the one in `infra/s3.tf`.

State records secrets in plain text, which is why the bucket is private, encrypted and
TLS-only. Never commit a state file; `.gitignore` already excludes `*.tfstate`.

## First-time setup

Prerequisites: AWS CLI logged in to the group account, Terraform >= 1.10, Docker,
and a Kaggle API token (kaggle.com -> Settings -> API Tokens -> Generate New Token,
which shows a `KGAT_...` string once).

```bash
# 1. Create the infrastructure
cd infra
terraform init
terraform apply
cd ..

# 2. Store the Kaggle token (only once, not in Terraform state).
#    `read -s` hides what you paste and keeps it out of shell history.
read -rs KAGGLE_TOKEN   # paste the KGAT_... token, press Enter
aws secretsmanager put-secret-value \
  --secret-id is459-team6/kaggle \
  --secret-string "$KAGGLE_TOKEN"
unset KAGGLE_TOKEN

# 3. Build and push the ingest image
scripts/push_ingest_image.sh

# 4. Run the pipeline
aws stepfunctions start-execution \
  --state-machine-arn "$(terraform -chdir=infra output -raw pipeline_state_machine_arn)"
```

Watch progress in the Step Functions console. Task logs are in the CloudWatch log
group `/ecs/is459-team6-ingest`.

## Options (`infra/variables.tf`)

| Variable | Default | Notes |
|---|---|---|
| `dataset_variants` | `["HI"]` | Add `"LI"` for the low-illicit-ratio files |
| `dataset_sizes` | `["Medium", "Large"]` | |
| `raw_prefix` | `ibm-aml/v1` | Change to `v2` to force a fresh ingest |
| `curated_prefix` | `ibm-aml/v2` | v2 preserves stable transfer IDs and separate transfer labels |
| `enable_daily_schedule` | `false` | Turn on once the full pipeline exists |
| `enable_scoring` | `false` | Enable after publishing the model bundle and scoring image |
| `scoring_image_digest` | empty | Required immutable ECR digest when scoring is enabled |
| `force_destroy_buckets` | `false` | Set `true` before `terraform destroy` at project end |

## Frozen GIN scoring

`scoring/pipeline.py` integrates the selected enriched IBM GINe without changing its
weights or fitted transformations. It consumes curated **transfers**, not the existing
seven-day account-day feature table. Graph inputs contain 35 edge features and 30 node
features over cumulative history since 1 Sep 2022, including self-transfers. The
original feature functions and IBM model source are preserved byte-for-byte under
`scoring/vendor/`; `.gitattributes` protects their provenance hashes across checkouts.

Curated v2 adds `source_file`, `source_sha256`, `source_row_id` (zero-based before
filtering) and `transfer_id` (`sha256:row`) to every transfer. Preparation orders the
single-line IBM CSV by physical byte offsets before numbering, so Spark input splits
cannot change identity. It hashes the actual source bytes. Ground truth is written
separately to `transfer_labels`, while account-day labels retain their original grain.
Existing v1 output remains readable but cannot feed the graph adapter.

Install `scoring/requirements.txt` in a separate model environment. GPU replay needs
the matching CUDA PyTorch build (the original environment used `2.5.1+cu121`). Spark
tests continue to use Python 3.11 and `prepare/requirements-dev.txt`.

Run these commands from the repository root, substituting your source artifact paths:

```powershell
python -m scoring.pipeline bundle --source "C:/Users/User/Documents/Big Data/miscellaneous/gin_v2" --base "C:/Users/User/Documents/Big Data/miscellaneous/full_run" --out artifacts/gin-selected

# First validate the frozen inference path on the original historical graph.
python -m scoring.pipeline replay --base "C:/Users/User/Documents/Big Data/miscellaneous/full_run" --context "C:/Users/User/Documents/Big Data/miscellaneous/gin_v2/test_context" --model artifacts/gin-selected --expected "C:/Users/User/Documents/Big Data/miscellaneous/gin_v2/test_inference/test_scores.npy" --out artifacts/replay --device cuda

# Stage the complete curated v2 prefix locally, including its _SUCCESS marker.
python -m scoring.pipeline prepare --curated artifacts/curated-v2 --model artifacts/gin-selected --start 2022-09-14 --cutoff 2022-09-15 --out artifacts/day14-context
python -m scoring.pipeline infer --context artifacts/day14-context --model artifacts/gin-selected --out artifacts/day14-inference --device cuda

# K is explicit: 1,000 here is an example daily budget, not a validated daily policy.
python -m scoring.pipeline rank --context artifacts/day14-context --inference artifacts/day14-inference --out artifacts/day14-ranking --daily --k 1000

# Optional; scoring and export never read these labels.
python -m scoring.pipeline evaluate --context artifacts/day14-context --ranking artifacts/day14-ranking --labels artifacts/curated-v2/transfer_labels --out artifacts/day14-evaluation
```

Daily windows must run midnight to midnight. Only accounts with a non-self transfer
that day are eligible, but their score includes all incident transfers, including
self-transfers. Output contains `accounts.csv`, `transfers.parquet` and a manifest.
Scores are uncalibrated; evidence identifies the highest-scoring observed transfer.
Fewer than K eligible accounts produce a shorter list. Evaluation reports the actual
number reviewed. Historical evaluation-window metrics do not establish daily accuracy.

For a bank view, pass `--bank BANK_ID` to `prepare` to filter visibility before features
and sampling indexes, and to `rank` to restrict the review cohort. For the comparable
shared run, omit it from `prepare` but retain it in `rank`. Match window, budget and
eligibility between runs. See ADR-0004.

Each command refuses an existing output directory and writes `_SUCCESS` last. Bundles
verify model, fitted-feature, node-map and source-code hashes. Keep `artifacts/` out of
Git. The adapter uses disk-backed DuckDB joins/sorting and batched Parquet reads,
then builds memory-mapped numeric arrays with the original frozen feature functions.
Inference uses memory-mapped arrays and batched sampling. `scoring/cloud.py` provides
S3 staging and publication inside an AWS Batch CPU job, and Terraform wires that job
after Prepare. `enable_scoring` defaults to false until the image and model bundle are
published. Keep the daily schedule off while validating a manual execution.

For the complete deployment and ingestion-to-prediction test, follow
[the scoring runbook](docs/scoring-runbook.md). `scripts/Publish-Image.ps1` publishes
either container image; `python -m scoring.publish` publishes an immutable model
bundle; `scripts/Test-Pipeline.ps1` starts an execution and verifies the gold outputs.

Validation on the supplied historical arrays reproduced 5,001,944 scores within
3.28e-7 maximum absolute error and exactly reproduced top-1,000 account IDs and their
supporting source rows. This validates checkpoint inference replay, not a full-sized
curated-to-graph rebuild or daily-forward performance.

The full rebuilt Parquet adapter matched all 35 saved feature columns exactly across
25,008,009 historical transfers. Local validation reports and benchmark outputs are
retained under the ignored `artifacts/` directory. See
[scoring validation](docs/scoring-validation.md) for daily metrics, compute sizing,
and the status of the cloud acceptance test.

# AML review dashboard (Part 5)

A Streamlit app with one page per business question.

| Page | Question | Reads |
|---|---|---|
| **Review queue** | Which accounts should investigators review first? | the BQ1 queue (`date=<day>/accounts.csv`, `ranking.json`), `metrics/`, curated `rings/` |
| **Account investigation** | Which transaction patterns justify that priority? | the queue, curated `transfers/`, `transfer_labels/` |
| **Sharing experiment** | How much laundering is invisible to a single bank, and how much more would sharing detect? | curated `rings/`, `sharing/comparison.csv` |

The investigation page has the **Shared view / Bank view** toggle. In bank view only transfers where the
account's own bank is the sender's or the receiver's bank are kept (the glossary's *bank view*), so the
account's own transfers stay but second-hop transfers between other banks disappear.

## Run it on the sample data (no AWS needed)

```bash
cd dashboard
pip install -r requirements.txt
streamlit run app.py
```

The default is `DASH_SOURCE=sample`, which reads `sample_data/`. **Everything in it is synthetic** (generated by
`scripts/make_sample_data.py` in the same formats the pipeline publishes) and the app says so in a banner.
Rebuild it with `python scripts/make_sample_data.py`.

## Run it on the real pipeline output

The app reads four folders. Each can be a local path or an `s3://` URI.

```
<root>/queue/     date=<day>/accounts.csv  date=<day>/ranking.json  (one BQ1 execution's queues)
<root>/metrics/   models.json  rules.json  rules_tuned.json          (the BQ1 jobs' metrics.json)
<root>/curated/   transfers/  transfer_labels/  rings/               (the curated version prefix)
<root>/sharing/   comparison.csv                                     (Person 4's results)
```

The BQ1 queue is published at `s3://is459-team6-gold-207567757101/bq1/queue/<execution>/date=<day>/` by the
pipeline's score step. It has no per-transfer scores and no evaluation: the score job reads no labels by
design. Precision and recall come from the training and baseline jobs' `metrics.json` instead.

Pull the files down with the helper script. For each day it refuses a queue without `_SUCCESS`, reads the
model named in `ranking.json` and takes that model's `metrics.json`, plus the two rules baselines'. Each of
those files is checked against its S3 ETag (its MD5). It then copies the curated transfers and labels for 1 to
16 Sep (history up to the last queue day) and the rings. Account `207567757101`, `us-east-1`; the defaults point
at execution `bq1-bankvis-20261109` (15 and 16 Sep) over curated `ibm-aml/e2e-20261105-152512`.

```bash
cd dashboard
aws sts get-caller-identity                  # confirm you are logged in (set AWS_PROFILE if you use a profile)
python scripts/pull_from_s3.py --dry-run     # shows the file count and total size, downloads nothing
python scripts/pull_from_s3.py               # copies into ./data (re-run to resume)

DASH_SOURCE=local DASH_ROOT=./data streamlit run app.py
```

Use `--execution`, `--days`, `--rules`, `--rules-tuned`, `--curated`, `--from`, `--to` and `--dest` to point at
a different run or date range. Put Person 4's `comparison.csv` in `data/sharing/`.

To read straight from S3 instead, set `DASH_SOURCE=s3` and `DASH_QUEUE_DIR`, `DASH_METRICS_DIR`,
`DASH_CURATED_DIR` and `DASH_SHARING_DIR` to `s3://...` prefixes. DuckDB uses your AWS credentials. Reading the 32M-row transfers
table over S3 on every click is slow, so for demos copy the files locally first.

### `sharing/comparison.csv` (the contract with the BQ2 experiment)

One row per bank and view. Each row is the `evaluation.json` that `scoring.pipeline evaluate` writes for one
run: `view = bank` for a run made with `--bank`, `view = shared` for the matching run without it (same window,
budget and eligible accounts, ADR-0004).

```csv
bank_id,view,k,reviewed,eligible,positive_accounts,true_positives,precision_at_k,recall_at_k
010,bank,30,30,4000,25,6,0.20,0.24
010,shared,30,30,4000,25,14,0.47,0.56
```

Until this file exists the Sharing page shows section 1 (visibility, computed from the rings table) and a
placeholder for section 2.

## What has and has not been verified

- The app, the data layer and the visibility analysis are tested on the sample data (`python -m pytest tests`).
- All three pages were run headless against the real files pulled for execution `bq1-bankvis-20261109`. Every
  queue account's (`bank_id`, `account_id`) joins to curated (`bank_id`, `account_number`), leading zeros
  included, and the precision and recall shown for 15 and 16 Sep equal `docs/bq1-results.md`.
- The queue is ranked by gradient-boosted trees trained on EMR, using only what the account's own bank can see.
  Scores rank accounts; they are not probabilities. The three reasons are descriptive flags (the account's most
  extreme features that day), not model attributions.
- The **known ring pattern** column and the orange edges are ground truth from the dataset's patterns file, shown
  for evaluation only and labelled as such.
- Section 1 of the Sharing page measures *visibility* (does one bank see all of a ring's transfers), not
  *detection*. Detection comes from section 2.

## Layout

```
app.py                      navigation
views/                      the three pages
dashlib/config.py           where data is read from
dashlib/data.py             every read (DuckDB); pages never touch files
dashlib/analysis.py         ring visibility, bank view, uplift (pure functions, tested)
dashlib/charts.py           Plotly figures
scripts/make_sample_data.py synthetic data generator
scripts/pull_from_s3.py     downloads the real BQ1 queues, metrics and curated files from S3
sample_data/                the generated sample (2 MB)
tests/                      unit tests and a headless run of every page
```

