# IS459_team6

AML detection pipeline on AWS (us-east-1), built with Terraform.

## Layout

```
infra/                 Terraform (S3 raw zone, ingest task, Step Functions, schedule)
ingest/                Fargate container: Kaggle -> S3 raw zone
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
     -> Prepare (placeholder for the EMR Serverless job)
```

The dataset is static, so ingestion really runs once. Daily runs skip it. If the
task fails partway, a re-run skips files already in S3 and carries on.

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
| `enable_daily_schedule` | `false` | Turn on once the full pipeline exists |
| `force_destroy_buckets` | `false` | Set `true` before `terraform destroy` at project end |
