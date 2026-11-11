#!/usr/bin/env bash
# Delete every project resource at the end of the course, then check that none is left
# (the runbook is docs/teardown.md):
#   1. a targeted apply that turns on force_destroy_buckets, which only takes effect once in state;
#   2. terraform destroy with the same variable;
#   3. every version and delete marker in the hand-made state bucket, then the bucket;
#   4. a sweep for anything still tagged Project=is459-team6, which fails if one remains.
# A re-run skips the steps already done.
# Usage (repository root, AWS credentials for the team account, infra/ already initialised):
#   scripts/teardown.sh [--dry-run]    # --dry-run prints each change; read-only lookups still run
set -euo pipefail
cd "$(dirname "$0")/.."
export AWS_PAGER=""

DRY_RUN=false
case "${1:-}" in
  "") ;;
  --dry-run) DRY_RUN=true ;;
  *) echo "Usage: scripts/teardown.sh [--dry-run]" >&2; exit 1 ;;
esac
run() { if $DRY_RUN; then echo "+ $*"; else "$@"; fi; }

PROJECT=is459-team6
# Both from the backend block, so a re-run works after the destroy has emptied the outputs.
STATE_BUCKET="$(sed -nE 's/^ *bucket *= *"([^"]+)".*/\1/p' infra/providers.tf)"
REGION="$(sed -nE 's/^ *region *= *"([^"]+)".*/\1/p' infra/providers.tf)"
[[ -n "$STATE_BUCKET" && -n "$REGION" ]] || { echo "No backend bucket or region found in infra/providers.tf" >&2; exit 1; }
aws() { command aws --region "$REGION" "$@"; }
# tr: the Windows AWS CLI ends text output with CRLF.
ACCOUNT="$(aws sts get-caller-identity --query Account --output text | tr -d '\r')"

echo "Account: $ACCOUNT"
echo "Region:  $REGION"
echo "This deletes every $PROJECT resource, all its data and the state bucket $STATE_BUCKET."
IFS= read -rp "Type the account ID to continue: " TYPED
[[ "${TYPED%$'\r'}" == "$ACCOUNT" ]] || { echo "Account ID does not match: nothing was changed." >&2; exit 1; }

# A re-run resumes: no state bucket means steps 1-3 are done, empty state means steps 1-2 are.
if aws s3api head-bucket --bucket "$STATE_BUCKET" >/dev/null 2>&1; then
  STATE="$(terraform -chdir=infra state list | tr -d '\r')"
  if [[ -n "$STATE" ]]; then
    echo; echo "== 1. Allow Terraform to delete non-empty buckets"
    # Every resource that reads force_destroy_buckets.
    run terraform -chdir=infra apply -auto-approve -var force_destroy_buckets=true \
      -target=aws_s3_bucket.raw -target=aws_s3_bucket.curated -target=aws_s3_bucket.scoring \
      -target=aws_athena_workgroup.this -target=aws_ecr_repository.scoring

    echo; echo "== 2. Destroy everything Terraform manages"
    run terraform -chdir=infra destroy -var force_destroy_buckets=true
  else
    echo; echo "== 1-2. Terraform state is already empty: skipped"
  fi

  echo; echo "== 3. Empty and delete the state bucket"
  # A dry run changed nothing, so its state is still full; only a real run must find it empty.
  STATE="$(terraform -chdir=infra state list | tr -d '\r')"
  if [[ -n "$STATE" ]] && ! $DRY_RUN; then
    echo "$STATE"
    echo "Terraform state still lists the resources above, so the state bucket is kept." >&2
    echo "Fix the destroy and run this script again." >&2
    exit 1
  fi
  for kind in Versions DeleteMarkers; do
    while :; do
      # 250 per batch keeps the JSON argument under the Windows command-line limit.
      BATCH="$(aws s3api list-object-versions --bucket "$STATE_BUCKET" --max-items 250 \
        --query "{Objects: ${kind}[].{Key: Key, VersionId: VersionId}, Quiet: \`true\`}" --output json)"
      [[ "$BATCH" == *'"Key"'* ]] || break
      # Quiet mode prints only failures, and still exits 0 when some keys fail.
      RESULT="$(run aws s3api delete-objects --bucket "$STATE_BUCKET" --delete "$BATCH" --output json)"
      [[ -z "$RESULT" ]] || echo "$RESULT"
      [[ "$RESULT" != *'"Errors"'* ]] || { echo "Some versions could not be deleted (above)." >&2; exit 1; }
      ! $DRY_RUN || break
    done
  done
  run aws s3api delete-bucket --bucket "$STATE_BUCKET"
else
  echo; echo "== 1-3. State bucket $STATE_BUCKET is already gone: skipped"
fi

echo; echo "== 4. Resources still tagged Project=$PROJECT"
LEFT="$(aws resourcegroupstaggingapi get-resources --tag-filters "Key=Project,Values=$PROJECT" \
  --query 'ResourceTagMappingList[].ResourceARN' --output text | tr -d '\r')"
if [[ -n "$LEFT" ]]; then
  echo "$LEFT" | tr '\t' '\n'
  echo "Resources remain: see docs/teardown.md for what to do with them." >&2
  exit 1
fi
echo "None. Finish the manual steps in docs/teardown.md."
