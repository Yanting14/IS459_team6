#!/usr/bin/env bash
# Build the scoring image (GIN scoring + the BQ2 compare job) and push it to ECR, then print its
# digest for `scoring_image_digest`. The macOS/Linux counterpart of Publish-Image.ps1 -Component scoring.
# Run after `terraform apply`, from anywhere in the repository, with Docker running.
# Usage: scripts/push_scoring_image.sh <tag>      e.g. scripts/push_scoring_image.sh gin-bq2-20261110
set -euo pipefail
cd "$(dirname "$0")/.."

TAG="${1:?usage: scripts/push_scoring_image.sh <tag>  (tags are immutable: use a new one each time)}"
[[ "$TAG" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]] || { echo "Invalid image tag: $TAG" >&2; exit 1; }
REPO_URL="$(terraform -chdir=infra output -raw scoring_ecr_repository_url)"
REGION="$(terraform -chdir=infra output -raw region)"

aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "${REPO_URL%%/*}"

# Batch runs on x86_64 (m7i); build for it explicitly so Apple Silicon Macs work too.
# The build context is the repository root: the image needs scoring/ and bq2/ (see .dockerignore).
docker build --platform linux/amd64 -f scoring/Dockerfile -t "$REPO_URL:$TAG" .
docker push "$REPO_URL:$TAG"

DIGEST="$(aws ecr describe-images --region "$REGION" --repository-name "${REPO_URL#*/}" \
  --image-ids imageTag="$TAG" --query 'imageDetails[0].imageDigest' --output text)"
[[ "$DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "ECR returned no digest for $TAG" >&2; exit 1; }
echo "Published $REPO_URL:$TAG"
echo "Set in infra/team.auto.tfvars.json:  \"scoring_image_digest\": \"$DIGEST\""
