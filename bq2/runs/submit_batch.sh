#!/usr/bin/env bash
# Submit the BQ2 bank-view GIN runs straight to AWS Batch, skipping Step Functions and its
# Prepare step (curated is already built, and Prepare needs EMR capacity that other jobs may hold).
# Same job definition and environment overrides as the pipeline's Score state; Batch queues them.
# Usage (from bq2/runs): ./submit_batch.sh banks_final.csv [dates...]
# Appends "<execution_id>\t<job_id>" to jobs.tsv and skips ids already in it, so it is safe to re-run.
set -u
export AWS_REGION=${AWS_REGION:-us-east-1}   # set AWS_PROFILE yourself if you use a named profile
QUEUE=is459-team6-scoring
JOBDEF=is459-team6-scoring:2
BANKS=$1; shift
DATES=${*:-2022-09-15 2022-09-16}
touch jobs.tsv

for bank in $(tail -n +2 "$BANKS" | cut -d, -f1); do
  for day in $DATES; do
    id="bq2-bank-$bank-$day"
    if grep -q "^$id	" jobs.tsv; then echo "skip $id (already submitted)"; continue; fi
    cutoff=$(python3 -c "import datetime as d,sys; print((d.date.fromisoformat(sys.argv[1])+d.timedelta(days=1)).isoformat())" "$day")
    env="[{\"name\":\"SCORING_START\",\"value\":\"$day\"},{\"name\":\"SCORING_CUTOFF\",\"value\":\"$cutoff\"},{\"name\":\"SCORING_K\",\"value\":\"1000\"},{\"name\":\"SCORING_BANK\",\"value\":\"$bank\"},{\"name\":\"EXECUTION_ID\",\"value\":\"$id\"}]"
    job=$(aws batch submit-job --job-name "is459-team6-scoring-$id" --job-queue "$QUEUE" \
          --job-definition "$JOBDEF" --container-overrides "{\"environment\":$env}" --query jobId --output text) \
      && printf '%s\t%s\n' "$id" "$job" >> jobs.tsv && echo "submitted $id $job"
  done
done
