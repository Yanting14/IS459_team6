# Frozen GIN scoring validation

The selected checkpoint remains `6e5e2da7ed914e14`, with its original weights,
training-fitted scalers and category dictionaries. No daily labels are used for
preparation, inference, or ranking. Evaluation joins labels only after publication.

## Local full-data validation

The source CSV SHA-256 is
`3126afb8155e7c8815d62bc5370549a7b5ae6bf7dfe872b3cd7813e66a3d7ff5`.
The benchmark staged all 31,898,238 source rows into transfer-only Parquet and a
separate label table. This converter is a benchmark fixture, not the full Spark
curated job: it does not produce account-day features, ring tables or quality reports.

The disk-backed adapter reproduced chronological edges, timestamps and source rows
for all 26,938,966 transfers visible before 15 September. All 35 edge feature columns
matched the original saved test-context features exactly over their full shared
prefix of 25,008,009 transfers (`artifacts/full-feature-parity.json`). The previously
completed checkpoint replay reproduced 5,001,944 transfer scores within 3.28e-7,
with identical top-1,000 account IDs and evidence transfers.

Each benchmark runs preparation, CPU inference and ranking in one process, matching
the AWS worker lifetime. Memory includes the Windows virtualenv launcher and child
interpreter. The host has 32 GiB RAM; inference uses four Torch threads, batch size
4,096, neighbours `[10, 5]`, and seed 1. These are workstation timings, not AWS SLAs.

| Day | Context transfers | Scored transfers | Preparation | Inference | Peak resident memory |
|---|---:|---:|---:|---:|---:|
| 2022-09-14 | 26,938,966 | 1,930,957 | 148.0 s | 118.5 s | 6.59 GiB |
| 2022-09-15 | 28,869,385 | 1,930,419 | 143.8 s | 114.0 s | 7.04 GiB |
| 2022-09-16 | 31,891,251 | 3,021,866 | 149.0 s | 136.2 s | 7.75 GiB |

Daily review budget is fixed at K=1,000. An account is eligible if it participates
in a non-self transfer that day; ranking uses its maximum incident transfer score,
including self-transfers. Positive accounts participate in at least one labelled
laundering transfer that day and meet the same eligibility rule. The same account
can appear on multiple days; daily counts must not be summed as unique people.

| Day | Eligible accounts | Positive accounts | True positives in top 1,000 | Precision@1,000 | Recall@1,000 |
|---|---:|---:|---:|---:|---:|
| 2022-09-14 | 900,182 | 3,524 | 822 | 82.20% | 23.33% |
| 2022-09-15 | 899,565 | 3,632 | 807 | 80.70% | 22.22% |
| 2022-09-16 | 1,421,148 | 3,757 | 723 | 72.30% | 19.24% |

These are simulated-data daily retrospective results. They do not establish
calibrated probabilities or real-world detection performance. The prior multi-day
historical evaluation and daily review policy have different eligibility and budgets.

## Compute choice

Use one `m7i.xlarge` CPU worker: 4 vCPU and 16 GiB RAM, with a 12 GiB container limit
and 100 GiB encrypted gp3 storage. This is the smallest M7i size that preserves the
tested four-thread execution and provides memory headroom above the measured graph.
It is not a claim that every smaller or differently shaped instance was benchmarked.
See [AWS M7i specifications](https://aws.amazon.com/ec2/instance-types/m7i/).

The Batch environment has zero minimum vCPUs. It uses the ECS AL2023 CPU AMI and a
CPU Torch image; no GPU quota or GPU reservation is needed. Keep the daily schedule
off until a manual cloud run is accepted. Startup, S3 staging, disk cache and EC2
CPU differences can make cloud runtime differ from this local measurement.

## Reproduce the local benchmark

Install `scoring/requirements-dev.txt` in a model environment. Keep artifacts outside
Git and use fresh output directories. The source must match the selected model hash.

```powershell
python -m scoring.benchmark stage-csv --source 'PATH/HI-Medium_Trans.csv' --model artifacts/gin-selected --out artifacts/benchmark-curated
python -m scoring.benchmark day --curated artifacts/benchmark-curated --model artifacts/gin-selected --out artifacts/benchmark-cpu-2022-09-14 --date 2022-09-14 --device cpu
python -m scoring.benchmark day --curated artifacts/benchmark-curated --model artifacts/gin-selected --out artifacts/benchmark-cpu-2022-09-15 --date 2022-09-15 --device cpu
python -m scoring.benchmark day --curated artifacts/benchmark-curated --model artifacts/gin-selected --out artifacts/benchmark-cpu-2022-09-16 --date 2022-09-16 --device cpu
```

Each day has `summary.json`, resource reports, logs, `context/`, `inference/`,
`ranking/accounts.csv`, `ranking/transfers.parquet`, and `evaluation/evaluation.json`.
Successful stages can be reused only for the same recorded request.

## AWS acceptance

The full manual AWS acceptance test **passed on 5 November 2026**. The execution
`scoring-e2e-20261105-152512` ran from 17:27:58 to 17:56:09 Malaysia time (28 min
12 s). It performed fresh ingestion, the complete Spark curated job, and September
14 scoring/publication. September 15 and 16 were evaluated locally, not rerun in AWS.

| Stage | Observed result |
|---|---|
| Ingest | Fresh HI-Medium ingestion completed in approximately 1 min 43 s |
| Prepare | EMR `SUCCESS`; 16 min 48 s execution time, excluding startup |
| Quality | Schema v2; 31,898,238 accepted rows, zero quarantined |
| Score | Batch `SUCCEEDED` on CPU; container runtime 3 min 56 s |
| Output | Five published artifacts passed SHA-256 verification; 1,000 ranked accounts |
| Parity | All 1,930,957 cloud scores matched local CPU scores within 4.77e-7 |
| Ranking | Top-1,000 account IDs and supporting transfer IDs matched exactly |
| Idle configuration | Batch desired capacity returned to zero; daily schedule remains disabled |

The Batch container staged 28 Parquet files and eight model files. Graph preparation
took approximately 116 s and inference 100 s. Batch queue/startup added about 3 min
33 s before the container started. Spark's dynamic allocator requested more workers
than its 32-vCPU application cap; allocation warnings occurred, but seven executors
continued processing and the job completed successfully without increasing the cap.
EMR reported billed usage of 8.247 vCPU-hours and 32.987 GB-hours of memory; these
usage quantities are not a total cost estimate.

Deployment identifiers:

- Account/region: `207567757101` / `us-east-1`.
- Raw and curated prefix: `ibm-aml/e2e-20261105-152512` in their respective buckets.
- Model: `s3://is459-team6-models-207567757101/gin/6e5e2da7ed914e14`.
- Image: `207567757101.dkr.ecr.us-east-1.amazonaws.com/is459-team6-scoring@sha256:011fdde640767a2e7d2abdff9f4519dcd7e255ea5479bb10f926732f60a8cd4f`.
- Execution ARN: `arn:aws:states:us-east-1:207567757101:execution:is459-team6-pipeline:scoring-e2e-20261105-152512`.
- EMR job: `00ga2n6qlh8o6g0b` in application `00ga2119lltms209`.
- Batch job: `31b3e3a6-77c3-43ae-8503-8a58bf3788d5`.
- Gold receipt: `s3://is459-team6-gold-207567757101/ibm-aml/e2e-20261105-152512/scoring/executions/scoring-e2e-20261105-152512.json`.

The receipt names the immutable prediction files. Local copies, checksums, provenance,
quality report and parity report are in `artifacts/scoring-e2e-20261105-152512/`.
The deployed overrides are in the ignored `infra/scoring-test.auto.tfvars.json`;
preserve/review these before the next apply, since repository defaults leave scoring
disabled and use different raw/curated prefixes. No model retraining or daily-budget
tuning was performed using these evaluation results.

Validation checks: 16 scoring tests passed, three mocked Terraform tests passed,
Terraform validation/format checks passed, and the Linux CPU checkpoint smoke test
passed. The 37 Spark tests had passed during the preceding preparation changes.
Follow [the scoring runbook](scoring-runbook.md) to repeat the manual test.
