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
