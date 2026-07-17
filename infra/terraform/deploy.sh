#!/usr/bin/env bash
#
# deploy.sh — build both images in ACR, apply the Terraform plan, then point
# the Container App at the freshly-built API image.
#
# Usage:
#   cd infra/terraform && ./deploy.sh
#
# Prereqs: `az login` already done, terraform installed, run from
# infra/terraform/ (or anywhere — paths below are repo-root relative and
# resolved via SCRIPT_DIR).
#
# This script does NOT run automatically as part of CI or ticket
# verification — it performs real `az acr build` and `terraform apply`
# against the logged-in Azure subscription. Run it deliberately.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$SCRIPT_DIR"

echo "==> terraform init"
terraform init -input=false

echo "==> terraform apply (provisions RG, Event Hubs, ACR, Container Apps env — API image left at placeholder)"
terraform apply -input=false -auto-approve

ACR_NAME="$(terraform output -raw acr_login_server | cut -d. -f1)"
RESOURCE_GROUP="$(terraform output -raw resource_group_name)"
CONTAINER_APP_NAME="$(terraform output -raw container_app_name)"

echo "==> az acr build: api image"
az acr build \
  --registry "$ACR_NAME" \
  --image "fraud-api:latest" \
  --file "$REPO_ROOT/docker/Dockerfile.api" \
  "$REPO_ROOT"

echo "==> az acr build: pipeline image"
az acr build \
  --registry "$ACR_NAME" \
  --image "fraud-pipeline:latest" \
  --file "$REPO_ROOT/docker/Dockerfile.pipeline" \
  "$REPO_ROOT"

echo "==> updating Container App to the freshly-built api image"
az containerapp update \
  --name "$CONTAINER_APP_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --image "$ACR_NAME.azurecr.io/fraud-api:latest"

echo "==> done. API FQDN:"
terraform output -raw api_fqdn
echo
