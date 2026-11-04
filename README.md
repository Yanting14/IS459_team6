# IS459_team6

AML detection pipeline on AWS (us-east-1), built with Terraform.

## Layout

```
infra/                 Terraform (S3 raw + curated zones, ingest task, EMR Serverless,
                       Step Functions, schedule)
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

`prepare/prepare.py` reads the raw transfer file for one dataset size, sets aside the
rows whose data is broken, and writes clean transfers to the curated zone, then a
quality report, then `_SUCCESS`:

```
<curated>/<version-prefix>/
  transfers/date=YYYY-MM-DD/*.parquet    sorted by sender_bank_id
  quarantine/run_id=<run_id>/*.parquet   rejected rows, with reason, source file and row number
  quality/<run_id>.json                  rows_in, rows_accepted, rows_quarantined by reason
  _SUCCESS
```

A row is quarantined when a field is missing or blank, the timestamp cannot be parsed,
an amount is not a positive number, or the currency or payment format is not in the
dataset's known set. Being a self-transfer is not a reason on its own. If the
quarantined share exceeds `--quarantine-threshold` (default `0.01`), the job writes the
quality report and then fails, so the evidence survives the failure.

Quarantine is partitioned by run, not by date as ADR-0001 asks of the curated tables:
it is audit output rather than something to read a day at a time, and a row with an
unparseable timestamp has no date to partition on.

If `_SUCCESS` already exists under the version prefix, the job does nothing, so
bumping `curated_prefix` is what forces a fresh build.

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

On Windows no winutils or `HADOOP_HOME` is needed: `prepare/conftest.py` compiles
`WindowsLocalFileSystem.java`, a test-only stand-in for Hadoop's local filesystem,
which otherwise needs native Hadoop binaries there.

## First-time setup

Prerequisites: AWS CLI logged in to the group account, Terraform >= 1.5, Docker,
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
