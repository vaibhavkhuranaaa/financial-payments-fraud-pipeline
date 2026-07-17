# Provider + version pins for the fraud-pipeline Azure infra.
# No backend configured — state is local by default. For a shared/team
# setup, add an azurerm backend block here and re-init with -reconfigure.

terraform {
  required_version = ">= 1.5"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
  }
}

provider "azurerm" {
  features {}
}
