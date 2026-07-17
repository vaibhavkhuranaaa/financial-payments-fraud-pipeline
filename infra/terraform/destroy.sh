#!/usr/bin/env bash
#
# destroy.sh — tear down all Azure resources provisioned by this Terraform
# config. Event Hubs Standard is the main recurring cost (~$25-30/mo); run
# this whenever the stack isn't actively being demoed.
#
# Usage:
#   cd infra/terraform && ./destroy.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "This will destroy ALL resources in the fraud-pipeline resource group(s)"
echo "managed by this Terraform state. This cannot be undone."
read -r -p "Type 'destroy' to confirm: " CONFIRM

if [[ "$CONFIRM" != "destroy" ]]; then
  echo "Aborted — no changes made."
  exit 1
fi

terraform destroy -auto-approve
