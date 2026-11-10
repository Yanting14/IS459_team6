#!/usr/bin/env bash
# Start one end-to-end pipeline execution, wait for it, and check what it published:
#   1. the Step Functions execution succeeded;
#   2. the BQ1 queue for every bq1 day exists under bq1/queue/<execution>/;
#   3. the Athena table queue_files now points at that queue, and Athena returns its rows;
#   4. if BQ2 ran, sharing/comparison.csv exists, with its pooled totals printed.
# Usage (repository root, AWS credentials for the team account):
#   scripts/run_pipeline.sh [execution-name] [input-json]
#   scripts/run_pipeline.sh                                   # defaults from infra/team.auto.tfvars.json
#   scripts/run_pipeline.sh e2e-retrain '{"bq1":{"retrain":true}}'
set -euo pipefail
cd "$(dirname "$0")/.."
export AWS_PAGER=""

NAME="${1:-e2e-$(date +%Y%m%d-%H%M%S)}"
INPUT="${2:-}"
[[ -n "$INPUT" ]] || INPUT='{}'
[[ "$NAME" =~ ^[A-Za-z0-9_-]{1,80}$ ]] || { echo "Execution names: letters, digits, _ and -, up to 80" >&2; exit 1; }

tf() { terraform -chdir=infra output -raw "$1"; }
ARN="$(tf pipeline_state_machine_arn)"
REGION="$(tf region)"
GOLD="s3://$(tf scoring_gold_uri | sed -E 's#^s3://([^/]+)/.*#\1#')"
CURATED_PREFIX="$(tf scoring_gold_uri | sed -E 's#^s3://[^/]+/(.*)/scoring$#\1#')"
WORKGROUP="$(tf athena_workgroup)"
DATABASE="$(tf athena_database)"
aws() { command aws --region "$REGION" "$@"; }

echo "Starting $NAME with input $INPUT"
EXEC_ARN="$(aws stepfunctions start-execution --state-machine-arn "$ARN" --name "$NAME" \
  --input "$INPUT" --query executionArn --output text)"
echo "Console: https://$REGION.console.aws.amazon.com/states/home?region=$REGION#/v2/executions/details/$EXEC_ARN"

while :; do
  STATUS="$(aws stepfunctions describe-execution --execution-arn "$EXEC_ARN" --query status --output text)"
  echo "$(date +%H:%M:%S) $STATUS"
  [[ "$STATUS" == RUNNING ]] || break
  sleep 30
done
if [[ "$STATUS" != SUCCEEDED ]]; then
  aws stepfunctions describe-execution --execution-arn "$EXEC_ARN" --query '{error:error,cause:cause}'
  echo "FAILED: open the console link above, find the red state, and check its CloudWatch logs." >&2
  exit 1
fi
OUTPUT="$(aws stepfunctions describe-execution --execution-arn "$EXEC_ARN" --query output --output text)"

echo; echo "== BQ1 queue"
DAYS="$(python3 -c 'import json,sys; o=json.loads(sys.argv[1]); print(" ".join(o.get("bq1_request",{}).get("options",{}).get("dates",[])))' "$OUTPUT")"
if [[ -z "$DAYS" ]]; then
  echo "No BQ1 days scored (no pinned model and no retrain): skipping the queue checks."
else
  for day in $DAYS; do
    aws s3 ls "$GOLD/bq1/queue/$NAME/date=$day/accounts.csv" >/dev/null \
      && echo "ok   $GOLD/bq1/queue/$NAME/date=$day/accounts.csv" \
      || { echo "MISSING queue for $day" >&2; exit 1; }
  done

  echo; echo "== Athena"
  LOCATION="$(aws glue get-table --database-name "$DATABASE" --name queue_files --query Table.StorageDescriptor.Location --output text)"
  [[ "$LOCATION" == *"/bq1/queue/$NAME/" ]] && echo "ok   queue_files -> $LOCATION" \
    || { echo "queue_files points at $LOCATION, not this run" >&2; exit 1; }
  QID="$(aws athena start-query-execution --work-group "$WORKGROUP" --query-execution-context Database="$DATABASE" \
    --query-string 'SELECT "date", count(*) AS accounts FROM queue_accounts GROUP BY 1 ORDER BY 1' \
    --query QueryExecutionId --output text)"
  while :; do
    STATE="$(aws athena get-query-execution --query-execution-id "$QID" --query QueryExecution.Status.State --output text)"
    [[ "$STATE" == QUEUED || "$STATE" == RUNNING ]] || break
    sleep 2
  done
  [[ "$STATE" == SUCCEEDED ]] || { echo "Athena query $STATE" >&2; exit 1; }
  aws athena get-query-results --query-execution-id "$QID" \
    --query 'ResultSet.Rows[].Data[].VarCharValue' --output text | xargs -n2 printf '     %s  %s\n'
fi

echo; echo "== BQ2 sharing"
if python3 -c 'import json,sys; sys.exit(0 if json.loads(sys.argv[1]).get("bq2_enabled") else 1)' "$OUTPUT"; then
  TMP="$(mktemp)"
  aws s3 cp --only-show-errors "$GOLD/$CURATED_PREFIX/sharing/comparison.csv" "$TMP"
  python3 - "$TMP" <<'PY'
import csv, sys
rows = list(csv.DictReader(open(sys.argv[1])))
for view in ("bank", "shared"):
    r = [x for x in rows if x["view"] == view]
    tp, rev = sum(int(x["true_positives"]) for x in r), sum(int(x["reviewed"]) for x in r)
    print(f"ok   {view:6} view: {tp} of {rev} reviewed ({100 * tp / rev:.1f}% precision), {len(r)} banks")
PY
  rm -f "$TMP"
else
  echo "BQ2 is off (enable_bq2=false): skipped."
fi
echo; echo "Pipeline $NAME succeeded. Dashboard: cd dashboard && DASH_SOURCE=athena streamlit run app.py"
