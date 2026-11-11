# Run the pipeline end to end

This runbook takes the pipeline from an empty AWS account to the dashboard. If you only need to run the
pipeline that is already deployed in the team account, skip to [Run it](#2-run-it).

```
EventBridge (off) / manual start
  -> Step Functions  is459-team6-pipeline
     CheckRawZone (Lambda) -> Ingest (ECS Fargate, only the first time)   Kaggle -> S3 raw zone
     -> Prepare (EMR Serverless)                                          raw -> curated (skipped if built)
     -> TrainBQ1 (EMR, only if asked) -> ScoreBQ1 (EMR, one job per day)  Q1: ranked review queue -> gold
     -> PointQueueTable (Athena)                                          Athena now shows this run's queue
     -> Score (Batch, frozen GIN)                                         one shared-view scoring day -> gold
     -> BQ2Score (Batch, per bank and day) -> BQ2Compare (Batch)          Q2: bank vs shared view -> gold
  -> Athena tables + views (Glue catalog, made by Terraform)  -> Streamlit dashboard
```

Every step is idempotent: a re-run skips ingestion, skips Prepare when the curated version exists, and
reuses any GIN scoring run with identical inputs. A re-run of an already-built deployment costs cents and
mostly spends its time on the BQ1 EMR jobs.

## 0. Prerequisites

- AWS CLI v2 signed in to the team account `207567757101`, region `us-east-1`. Check with
  `aws sts get-caller-identity`. If you use a named profile, `export AWS_PROFILE=<profile>` first.
- Terraform 1.10 or newer.
- Python 3.11 with `boto3` (`pip install boto3`). Terraform runs `dashboard/athena/run_sql.py` to create the
  two Athena views.
- Docker, running, for the two container images (first deployment, or after changing `scoring/` or `bq2/`).
- First deployment only: a Kaggle API token (kaggle.com -> Settings -> API Tokens), and the frozen GIN model
  bundle (see the root README, "Frozen GIN scoring").

Commands are for macOS/Linux from the repository root. On Windows use the PowerShell equivalents in
`scripts/` (`Publish-Image.ps1`, `Test-Pipeline.ps1`) and [the scoring runbook](scoring-runbook.md).

## 1. Deploy (first time, or after changing infra/)

The team's settings are in `infra/team.auto.tfvars.json`: which data version (`raw_prefix`,
`curated_prefix`), the pinned BQ1 model, the GIN image digest, and the BQ2 banks and days.

```bash
# 1a. Infrastructure: buckets, EMR, Batch, Step Functions, Athena workgroup, Glue tables and views.
cd infra && terraform init && terraform apply && cd ..

# 1b. Kaggle token, once. Not in Terraform state; `read -s` keeps it off screen and out of history.
read -rs KAGGLE_TOKEN
aws secretsmanager put-secret-value --secret-id is459-team6/kaggle --secret-string "$KAGGLE_TOKEN"
unset KAGGLE_TOKEN

# 1c. Container images.
scripts/push_ingest_image.sh
scripts/push_scoring_image.sh gin-bq2-$(date +%Y%m%d)    # prints the digest to use next

# 1d. Frozen GIN model bundle, once (immutable; the bundle is built as in README "Frozen GIN scoring").
python -m scoring.publish --bundle artifacts/gin-selected --destination "$(terraform -chdir=infra output -raw scoring_model_uri)"
```

Then switch on scoring and BQ2 in `infra/team.auto.tfvars.json` and apply again:

```json
"enable_scoring": true,
"scoring_image_digest": "sha256:<digest printed by push_scoring_image.sh>",
"enable_bq2": true
```

```bash
cd infra && terraform apply && cd ..
```

Terraform refuses `enable_scoring` without an image digest, and `enable_bq2` without `enable_scoring`.
The BQ2 compare job runs in the scoring image, so the image must be one built after `bq2/cloud.py` was
added. An older digest fails at `BQ2Compare` with `No module named bq2`.

## 2. Run it

```bash
scripts/run_pipeline.sh
```

This starts one execution with the defaults from `team.auto.tfvars.json`, prints its console link, waits,
and then checks every output (section 3). Expected times on HI-Medium: ingest about an hour (first run only),
Prepare about 25 minutes (first build only), each BQ1 day a few minutes on EMR, each GIN scoring run under a
minute once its Batch instance is up.

Options go in the execution input:

```bash
scripts/run_pipeline.sh e2e-retrain '{"bq1": {"retrain": true}}'          # retrain BQ1 first
scripts/run_pipeline.sh e2e-days '{"bq1": {"dates": ["2022-09-16"]}}'     # score other days
```

Without the script: Step Functions console -> `is459-team6-pipeline` -> Start execution, input `{}`.

## 3. Check the results

`run_pipeline.sh` does these checks and stops at the first failure:

| Check | Where | Expect |
|---|---|---|
| Execution | Step Functions | `SUCCEEDED` |
| Q1 queue per day | `s3://<gold>/bq1/queue/<execution>/date=<day>/accounts.csv` | one file per bq1 day |
| Athena follows the run | Glue table `queue_files` location | `.../bq1/queue/<execution>/` |
| Athena returns it | `SELECT "date", count(*) FROM queue_accounts GROUP BY 1` | K rows (1,000) per day |
| Q2 comparison | `s3://<gold>/<curated_prefix>/sharing/comparison.csv` | bank view 165 of 320, shared view 210 of 320 on the team's 16 banks, 15-16 Sep |

The Q2 numbers are the ones in `bq2/runs/README.md`. The pipeline reuses identical scoring runs, so a
different result means the inputs changed (data version, model, image or banks).

## 4. Open the dashboard

```bash
cd dashboard
pip install -r requirements.txt
DASH_SOURCE=athena streamlit run app.py
```

The dashboard reads every table through Athena. It shows the queue the last run published, because that
run re-pointed `queue_files`. Restart Streamlit after a new run to clear its cache. Users need the IAM policy
from `terraform output athena_query_policy_arn`.

### Hosted

The same app can run on one EC2 instance behind a shared password. It is plain HTTP, so pick a password
used nowhere else. Set the password once (Terraform creates the secret but never its value), then turn
the host on:

```bash
cd infra
aws secretsmanager put-secret-value --secret-id "$(terraform output -raw dashboard_password_secret_name)" \
  --secret-string 'a-password-used-nowhere-else'
terraform apply -var enable_dashboard=true   # or set "enable_dashboard": true in team.auto.tfvars.json
terraform output -raw dashboard_url          # http://<public DNS>:8501, after a few minutes of boot
```

The service refuses to start while the secret is empty and retries every 30 seconds, so setting the
password later also works. A change under `dashboard/` replaces the instance on the next apply, and the
URL changes with it: re-read `dashboard_url`. Shell access is through SSM Session Manager only
(`aws ssm start-session --target <instance id>`); the boot log is `/var/log/cloud-init-output.log` and the
service log is `journalctl -u dashboard`. Turn it off again with `enable_dashboard=false`.

## 5. When something fails

| Failed state | Look at | Usual cause |
|---|---|---|
| `Ingest` | CloudWatch `/ecs/is459-team6-ingest` | Kaggle token missing or expired (1b) |
| `Prepare`, `TrainBQ1`, `ScoreBQ1` | `s3://<curated>/logs/` (Spark driver stderr) | quality gate tripped; EMR capacity |
| `PointQueueTable` | the Athena query history in the console | the pipeline role lacks Athena/Glue access: re-apply |
| `Score`, `BQ2Score` -> `ScoringFailed` / `BQ2Failed` | CloudWatch `/batch/is459-team6-scoring` | image digest or model bundle not published; EC2 quota for m7i.xlarge |
| `BQ2Compare` -> `BQ2Failed` | same log group, stream `bq2-compare/...` | image predates `bq2/` (rebuild, 1c); a bank or day with no receipt |

Fix the cause and run again: completed work is reused, so a re-run continues rather than starting over.

## Settings that change what runs

| Setting (`infra/team.auto.tfvars.json`) | Effect |
|---|---|
| `raw_prefix`, `curated_prefix` | a new value re-ingests / rebuilds curated under a new version |
| `bq1_model_version`, `bq1_dates`, `bq1_k` | the pinned Q1 model, the days it scores, its review budget |
| `enable_scoring`, `scoring_image_digest`, `scoring_start` | the GIN scoring step |
| `enable_bq2`, `bq2_dates`, `bq2_banks`, `bq2_ratio`, `bq2_floor` | the Q2 experiment |
| `enable_daily_schedule` | daily 02:00 SGT start; off, since the dataset is static |

The dashboard stays local (`streamlit run`). It is not hosted, so no public endpoint holds the data.
