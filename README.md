# IS459_team6

AML detection pipeline on AWS (us-east-1), built with Terraform.

## Layout

```
infra/                 Terraform (S3 raw + curated zones, ingest task, EMR Serverless,
                       Step Functions, schedule; state in S3)
ingest/                Fargate container: Kaggle -> S3 raw zone
prepare/               Spark job (EMR Serverless): raw zone -> curated zone
lambdas/check_raw/     Lambda: has the raw zone already been ingested?
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
  rings/*.parquet                        one row per transfer listed in the patterns file
  quarantine/run_id=<run_id>/*.parquet   rejected rows, with reason, source file and row number
  quality/<run_id>.json                  row counts and reasons, USD rates, self-transfers,
                                         and the days and banks the run saw
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

All twelve tail days together hold 6,987 rows, 0.022% of the file, so the window loses
no meaningful volume. It does lose signal out of proportion to its size: 58.5% of those
rows carry the laundering mark against 0.098% in the main period, which is 4,084 of the
dataset's 35,230 laundering transfers, or 11.6%. Those account-days would have nearly
empty 7-day lookback windows, so they are poor training rows, but the trade is a real
one and is recorded on #2 rather than decided here.

`--input` and `--output` take S3 URIs or local folders, which is how the same script
runs on EMR Serverless and under the local tests:

```bash
spark-submit prepare/prepare.py --input s3://<raw>/ibm-aml/v1   --output s3://<curated> --size medium --version-prefix ibm-aml/v1 --quarantine-threshold 0.01
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
| `curated_prefix` | `ibm-aml/v1` | Change to `v2` to force a fresh Prepare build |
| `enable_daily_schedule` | `false` | Turn on once the full pipeline exists |
| `force_destroy_buckets` | `false` | Set `true` before `terraform destroy` at project end |
