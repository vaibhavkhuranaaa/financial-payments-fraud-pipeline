variable "prefix" {
  description = "Short name prefix applied to all resources (e.g. 'fraudpl'). Keep lowercase/alphanumeric — used in globally-unique names (ACR, Event Hubs namespace)."
  type        = string
  default     = "fraudpl"
}

variable "location" {
  description = "Azure region for all resources."
  type        = string
  default     = "eastus2"
}

variable "tags" {
  description = "Common resource tags."
  type        = map(string)
  default = {
    project     = "financial-payments-fraud-pipeline"
    environment = "dev"
    managed_by  = "terraform"
  }
}

variable "api_image" {
  description = "Fully-qualified container image (ACR login server/repo:tag) for the Container App. Placeholder until the first `az acr build` — deploy.sh updates it post-build."
  type        = string
  default     = "mcr.microsoft.com/k8se/quickstart:latest"
}

# ---------------------------------------------------------------------------
# Optional demo module (ticket 09) — see demo.tf. Off by default: every
# resource in demo.tf is gated on enable_demo so `terraform plan`/`apply`
# with defaults is a strict no-op w.r.t. these resources. COST NOTE: this
# ticket is validate-only — see docs/tickets/09-demo-infra-ci.md. Do not
# `terraform apply` with enable_demo=true without asking the user first
# (Azure SQL serverless + a Container App are small but non-zero recurring
# costs while provisioned).
# ---------------------------------------------------------------------------

variable "enable_demo" {
  description = "Provision the optional recruiter-demo module (Azure SQL serverless bank DB + dashboard Container App) in demo.tf. Default false — local `make demo` (Docker Compose) covers the same demo without any Azure spend."
  type        = bool
  default     = false
}

variable "dashboard_image" {
  description = "Fully-qualified container image (ACR login server/repo:tag) for the dashboard Container App. Placeholder until the first `az acr build --file docker/Dockerfile.dashboard`."
  type        = string
  default     = "mcr.microsoft.com/k8se/quickstart:latest"
}

variable "demo_sql_admin_login" {
  description = "SQL admin login for the demo Azure SQL logical server."
  type        = string
  default     = "bankadmin"
}

variable "demo_sql_admin_password" {
  description = "SQL admin password for the demo Azure SQL logical server. No default on purpose — pass via TF_VAR_demo_sql_admin_password or a .tfvars file kept out of git; never commit a real value."
  type        = string
  default     = "Set-me-via-TF_VAR-or-tfvars-1!"
  sensitive   = true
}
