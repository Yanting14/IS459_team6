# Test the pipeline from ingestion to prediction output

The cloud route is `CheckRaw -> Ingest (when needed) -> Prepare -> Score -> ScoringResult`.
`Score` waits for one AWS Batch CPU job that stages the model and curated transfers,
prepares graph inputs, performs inference, ranks accounts, and publishes gold output.
This uses AWS's [synchronous Batch integration](https://docs.aws.amazon.com/step-functions/latest/dg/connect-batch.html).
The job never downloads labels. Run evaluation separately when measuring accuracy.

These steps use real AWS resources in the team's shared account. The local test suite
uses mocks and cannot establish IAM, EC2 capacity, or network behavior.
The manual cloud run passed on 5 November 2026; its timings, output locations and
local/cloud parity results are recorded in [scoring validation](scoring-validation.md).

## Run the existing deployment from another computer

Sign in to the team account `207567757101`, select `us-east-1`, and open
Step Functions -> `is459-team6-pipeline` -> Start execution. Supply:

```json
{"scoring":{"start":"2022-09-14","cutoff":"2022-09-15","k":1000,"bank":""}}
```

The deployed Batch job pulls its container image from ECR and downloads the frozen
model bundle from `s3://is459-team6-models-207567757101/gin/6e5e2da7ed914e14/`.
Triggering it through the console requires AWS permissions, but no local model,
Python environment, Docker installation or GPU. An identical completed request
reuses its published predictions. The following sections cover deployment changes
and testing from a checkout; they are not required just to start the existing pipeline.

## Prerequisites

- AWS CLI v2 authenticated to account `207567757101`, region `us-east-1`.
- Terraform >=1.10. On the current workstation a checksum-verified executable is at
  `artifacts/tools/terraform.exe`; another checkout can use an installed `terraform`.
- Docker Desktop running Linux containers; local GPU access is not needed to build.
- The Kaggle token is populated in the existing `is459-team6/kaggle` secret as described
  in the root README. Do not put the token in Terraform variables or the test JSON.
- The selected local bundle exists at `artifacts/gin-selected`. See the root README's
  `scoring.pipeline bundle` command if it needs to be created.
- AWS EC2 standard-instance capacity/quota sufficient for `m7i.xlarge` (4 vCPU,
  16 GiB RAM). The Batch job requests 4 vCPU and 12 GiB memory, with 100 GiB encrypted
  scratch storage. CPU sizing follows the full-data local benchmark and was verified
  by the cloud acceptance run recorded in [scoring validation](scoring-validation.md).

If using a named profile, set `$env:AWS_PROFILE='team6'` in the PowerShell session.
If that profile was created with `aws login` and Terraform reports no credentials,
use AWS's [credential-process compatibility setup](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sign-in.html).
On this workstation the ignored `artifacts/aws-config` contains a `team6-tools`
profile referencing the existing login (no access keys):

```powershell
$env:PATH = 'C:\Program Files\Amazon\AWSCLIV2;' + $env:PATH
$env:AWS_CONFIG_FILE = (Resolve-Path artifacts/aws-config).Path
$env:AWS_PROFILE = 'team6-tools'
```

Renew the underlying session with `aws login --profile team6` when it expires.

## 1. Choose whether ingestion should actually run

From PowerShell in the repository root:

```powershell
cd C:\Users\User\Documents\IS459_team6
$tf = (Resolve-Path .\artifacts\tools\terraform.exe).Path
aws sts get-caller-identity

# A new pair of prefixes exercises ingestion and preparation without deleting
# previous results. Only HI-Medium is needed for this selected model.
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$settings = @{
  enable_scoring = $false
  enable_daily_schedule = $false
  dataset_variants = @('HI')
  dataset_sizes = @('Medium')
  raw_prefix = "ibm-aml/e2e-$stamp"
  curated_prefix = "ibm-aml/e2e-$stamp"
  scoring_image_digest = ''
}
$settings | ConvertTo-Json | Set-Content -Encoding ascii infra/scoring-test.auto.tfvars.json
```

For a faster run that reuses existing ingestion, set `raw_prefix` to `ibm-aml/v1`
instead. Use a fresh curated prefix or an already completed **v2-schema** build.
Do not use the old v1 curated table: it lacks transfer IDs. Terraform auto variable
files persist across applies; keep one authoritative test settings file and review
the plan if other variable files already exist. Never remove success markers to force
this test; use a new prefix instead.

## 2. Bootstrap infrastructure and publish artifacts

```powershell
& $tf -chdir=infra init
& $tf -chdir=infra plan '-out=scoring-bootstrap.tfplan'
& $tf -chdir=infra apply scoring-bootstrap.tfplan

# Existing ingest image uses the default latest tag. Scoring tags are immutable;
# use a new tag for each build. These scripts build and push Linux/amd64 images.
.\scripts\Publish-Image.ps1 -Component ingest -Tag latest -Terraform $tf
.\scripts\Publish-Image.ps1 -Component scoring -Tag "gin-$stamp" -Terraform $tf

$modelUri = & $tf -chdir=infra output -raw scoring_model_uri
& .\.venv\Scripts\python.exe -m scoring.publish --bundle artifacts/gin-selected --destination $modelUri
```

The publisher validates the local bundle, refuses to replace different existing
artifacts at the same prefix, and writes the model `_SUCCESS` marker last. A retry
can finish an interrupted upload. For a different model, use a new model prefix and
set `scoring_model_version` accordingly. A fresh Python environment needs the scoring
dependencies plus boto3; the current workstation already has them.

Scoring remains disabled in this bootstrap apply. Batch compute has zero minimum
vCPUs and no jobs are submitted by applying the configuration alone.

## 3. Enable scoring and deploy the final state machine

```powershell
$published = Get-Content artifacts/scoring-image.json -Raw | ConvertFrom-Json
$settings.scoring_image_digest = $published.digest
$settings.enable_scoring = $true
$settings | ConvertTo-Json | Set-Content -Encoding ascii infra/scoring-test.auto.tfvars.json

& $tf -chdir=infra plan '-out=scoring-enabled.tfplan'
& $tf -chdir=infra apply scoring-enabled.tfplan
```

The image is pinned by digest. Terraform refuses `enable_scoring=true` without a
digest. The daily trigger remains off. Its future runs would replay the configured
historical day; it does not translate today's date into this static dataset.

## 4. Run and verify the whole pipeline

```powershell
.\scripts\Test-Pipeline.ps1 -Terraform $tf `
  -StartDate 2022-09-14 -Cutoff 2022-09-15 -K 1000
```

The script:

1. Starts the main Step Functions state machine with explicit scoring options.
2. Polls until the execution finishes; any pipeline failure fails the script.
3. Requires scoring output (a Prepare-only success does not pass).
4. Downloads the execution receipt and authoritative gold completion manifest.
5. Downloads all five published artifacts and verifies their SHA-256 checksums.
6. Checks the review budget and model version, then prints `PASS`.

Look in the printed `artifacts/scoring-test-<timestamp>/` directory for:

| File | Meaning |
|---|---|
| `accounts.csv` | Ranked eligible accounts, risk scores and supporting transfer IDs |
| `transfers.parquet` | Individual transfer scores |
| `ranking.json` | Window, cutoff, visibility, K and eligibility policy |
| `context_manifest.json` | Graph/source/model provenance and unknown-category counts |
| `inference.json` | Inference provenance and score checksum |
| `receipt.json`, `completion.json` | Authoritative output locations and checksums |
| `execution.json`, `result.json`, `input.json` | AWS execution details and input |

K=1,000 is a test review budget. A successful run establishes execution and output
integrity, not daily prediction accuracy. Fewer than K accounts is valid if fewer are
eligible. Scores are not calibrated probabilities.

To test idempotency, run the same command again with a new execution name (the default
generates one). Raw ingestion and Prepare should skip their completed prefixes; the
Batch worker should reuse the gold completion manifest. Batch startup can still occur.
To score 15 September, use `-StartDate 2022-09-15 -Cutoff 2022-09-16`. To exercise a
bank view, add `-Bank '010'`; visibility is filtered before graph construction.

## Output and failure behavior

Gold lives under `<curated-prefix>/scoring/predictions/date=<day>/run=<content-hash>/`.
The content hash includes image digest, model/input inventories and scoring options.
Each attempt uploads to its own subdirectory. Only a conditional `_SUCCESS` write
publishes that attempt's files. A reader must use the manifest's artifact URIs, not
glob every attempt: an interrupted or losing attempt can leave unreferenced files.
The execution receipt is under `<curated-prefix>/scoring/executions/<execution-name>.json`.

Check Step Functions execution history first when the test fails. Ingest logs are at
`/ecs/is459-team6-ingest`; Prepare logs are in the curated bucket's `logs/` prefix;
scoring logs are at `/batch/is459-team6-scoring`. A Batch job staying `RUNNABLE` often
requires checking compute-environment status, standard EC2 quota/capacity, public-subnet routing
and image access. Model/data errors are not retried; lost EC2 hosts get at most two
attempts. Each scoring attempt has a two-hour timeout, with an outer Step Functions
limit that also bounds queueing. A local test-script timeout stops polling but does
not cancel the AWS execution; its ARN is saved in `execution.json`.

## Local checks without a cloud run

```powershell
& .\.venv\Scripts\python.exe -m pytest scoring -v
& $tf -chdir=infra fmt -check -recursive
& $tf -chdir=infra validate
& $tf -chdir=infra test
```

On a fresh checkout, `terraform init -backend=false` downloads providers for these
local checks without initializing the shared S3 backend. Run normal `terraform init`
before the real deployment. The `.tftest.hcl` suite mocks all providers, including its
mock apply; it does not create AWS resources.
