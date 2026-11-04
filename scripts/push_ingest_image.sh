#!/usr/bin/env bash
# Build the ingest image and push it to ECR. Run after `terraform apply`.
# Usage: scripts/push_ingest_image.sh [tag]   (default tag: latest)
set -euo pipefail
cd "$(dirname "$0")/.."

TAG="${1:-latest}"
REPO_URL="$(terraform -chdir=infra output -raw ingest_ecr_repository_url)"
REGION="$(terraform -chdir=infra output -raw region)"

aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "${REPO_URL%%/*}"

# Fargate runs X86_64; build for it explicitly so Apple Silicon Macs work too.
docker build --platform linux/amd64 -t "$REPO_URL:$TAG" ingest
docker push "$REPO_URL:$TAG"
