# Optional recruiter-demo module (ticket 09): Azure SQL serverless (the
# managed equivalent of the local `bank-db` Azure SQL Edge container) +
# a dashboard Container App on the *existing* Container Apps environment/ACR
# provisioned in main.tf. Everything here is gated on `var.enable_demo`
# (default false) so a plain `terraform apply` with defaults touches none of
# it — the local `make demo` (Docker Compose) is the $0 way to run this demo;
# this module exists only for an occasional recruiter screen-share where a
# public URL is worth the small recurring cost while it's up.
#
# COST RULE: this ticket is validate-only. Do not `terraform apply` with
# -var enable_demo=true without asking the user first (~$/day for SQL
# serverless + Container App while provisioned; SQL auto-pauses after 60min
# idle, Container App does not scale to zero by default here).

# ---------------------------------------------------------------------------
# Azure SQL serverless — managed stand-in for the local bank-db container.
# ---------------------------------------------------------------------------

resource "azurerm_mssql_server" "demo" {
  count                        = var.enable_demo ? 1 : 0
  name                         = "${var.prefix}-bank-sql"
  resource_group_name          = azurerm_resource_group.main.name
  location                     = azurerm_resource_group.main.location
  version                      = "12.0"
  administrator_login          = var.demo_sql_admin_login
  administrator_login_password = var.demo_sql_admin_password
  minimum_tls_version          = "1.2"
  tags                         = var.tags
}

resource "azurerm_mssql_database" "bank" {
  count     = var.enable_demo ? 1 : 0
  name      = "bank"
  server_id = azurerm_mssql_server.demo[0].id

  # Serverless General Purpose, smallest Gen5 vCore family, auto-pause after
  # 60 minutes idle so a forgotten demo doesn't keep billing overnight.
  sku_name                    = "GP_S_Gen5_1"
  min_capacity                = 0.5
  max_size_gb                 = 5
  auto_pause_delay_in_minutes = 60
  zone_redundant              = false
  tags                        = var.tags
}

# Firewall: allow Azure-hosted resources (the dashboard Container App has no
# static/predictable egress IP unless VNet-integrated with a NAT gateway,
# which is out of scope for a demo module) via the special
# 0.0.0.0/0.0.0.0 "allow Azure services" rule. This is broader than a single
# Container Apps env IP, but matches what "firewall allowing the Container
# Apps env" resolves to without adding VNet integration + NAT gateway cost
# just for this demo. Tighten to a real VNet rule if this module ever runs
# longer than a demo session.
resource "azurerm_mssql_firewall_rule" "allow_azure_services" {
  count            = var.enable_demo ? 1 : 0
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.demo[0].id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

# ---------------------------------------------------------------------------
# Dashboard Container App — same environment/ACR as the api app in main.tf.
# ---------------------------------------------------------------------------

resource "azurerm_container_app" "dashboard" {
  count                        = var.enable_demo ? 1 : 0
  name                         = "${var.prefix}-dashboard"
  container_app_environment_id = azurerm_container_app_environment.main.id
  resource_group_name          = azurerm_resource_group.main.name
  revision_mode                = "Single"
  tags                         = var.tags

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.api.id]
  }

  registry {
    server   = azurerm_container_registry.main.login_server
    identity = azurerm_user_assigned_identity.api.id
  }

  template {
    min_replicas = 1
    max_replicas = 1

    container {
      name   = "dashboard"
      image  = var.dashboard_image
      cpu    = 0.5
      memory = "1Gi"

      env {
        name  = "BANK_DB_HOST"
        value = azurerm_mssql_server.demo[0].fully_qualified_domain_name
      }
      env {
        name  = "BANK_DB_PORT"
        value = "1433"
      }
      env {
        name  = "BANK_DB_USER"
        value = var.demo_sql_admin_login
      }
      env {
        name        = "BANK_DB_PASSWORD"
        secret_name = "bank-db-password"
      }
      env {
        name  = "BANK_DB_NAME"
        value = azurerm_mssql_database.bank[0].name
      }
      env {
        name  = "API_METRICS_URL"
        value = "https://${azurerm_container_app.api.ingress[0].fqdn}/metrics"
      }
      env {
        name  = "DASHBOARD_PORT"
        value = "8050"
      }
    }
  }

  secret {
    name  = "bank-db-password"
    value = var.demo_sql_admin_password
  }

  ingress {
    external_enabled = true
    target_port      = 8050
    transport        = "auto"

    traffic_weight {
      percentage      = 100
      latest_revision = true
    }
  }

  lifecycle {
    ignore_changes = [
      # deploy.sh (or an equivalent demo-deploy script) updates the image
      # out-of-band after `az acr build`; avoid terraform reverting it to
      # the placeholder on subsequent plans.
      template[0].container[0].image,
    ]
  }
}

# ---------------------------------------------------------------------------
# Outputs (only meaningful when enable_demo = true; empty string otherwise)
# ---------------------------------------------------------------------------

output "demo_dashboard_fqdn" {
  description = "Public FQDN of the demo dashboard Container App (empty when enable_demo = false)."
  value       = var.enable_demo ? azurerm_container_app.dashboard[0].ingress[0].fqdn : ""
}

output "demo_sql_fqdn" {
  description = "Fully-qualified domain name of the demo Azure SQL logical server (empty when enable_demo = false)."
  value       = var.enable_demo ? azurerm_mssql_server.demo[0].fully_qualified_domain_name : ""
}
