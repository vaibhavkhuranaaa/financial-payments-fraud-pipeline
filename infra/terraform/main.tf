# Azure infra for the fraud pipeline: Event Hubs (Kafka endpoint) for the
# transactions/DLQ topics, ACR for images, Container Apps for the /score API.
#
# Cost note (see README/report for detail): Event Hubs Standard is the
# dominant recurring cost (~$25-30/mo) because the Kafka protocol surface
# requires Standard tier (Basic doesn't expose it). Everything else here is
# either free-tier-eligible or scales to ~$0 at rest (Container Apps min
# replicas=1 keeps a small always-on cost too). Run destroy.sh when not
# actively demoing.

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "main" {
  name     = "${var.prefix}-rg"
  location = var.location
  tags     = var.tags
}

# ---------------------------------------------------------------------------
# Event Hubs (Kafka-compatible broker)
# ---------------------------------------------------------------------------

resource "azurerm_eventhub_namespace" "main" {
  name                = "${var.prefix}-ehns"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "Standard"
  capacity            = 1
  tags                = var.tags
}

resource "azurerm_eventhub" "transactions" {
  name                = "transactions"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 4
  message_retention   = 1
}

resource "azurerm_eventhub" "transactions_dlq" {
  name                = "transactions-dlq"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 1
  message_retention   = 1
}

resource "azurerm_eventhub_namespace_authorization_rule" "app" {
  name                = "${var.prefix}-app-rule"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  listen              = true
  send                = true
  manage              = false
}

# ---------------------------------------------------------------------------
# ACR
# ---------------------------------------------------------------------------

resource "azurerm_container_registry" "main" {
  name                = "${var.prefix}acr"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "Basic"
  admin_enabled       = false
  tags                = var.tags
}

# ---------------------------------------------------------------------------
# Log Analytics + Container Apps environment
# ---------------------------------------------------------------------------

resource "azurerm_log_analytics_workspace" "main" {
  name                = "${var.prefix}-law"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = var.tags
}

resource "azurerm_container_app_environment" "main" {
  name                       = "${var.prefix}-cae"
  location                   = azurerm_resource_group.main.location
  resource_group_name        = azurerm_resource_group.main.name
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id
  tags                       = var.tags
}

resource "azurerm_user_assigned_identity" "api" {
  name                = "${var.prefix}-api-identity"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = var.tags
}

resource "azurerm_role_assignment" "acr_pull" {
  scope                = azurerm_container_registry.main.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_user_assigned_identity.api.principal_id
}

resource "azurerm_container_app" "api" {
  name                         = "${var.prefix}-api"
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
    max_replicas = 2

    container {
      name   = "api"
      image  = var.api_image
      cpu    = 0.5
      memory = "1Gi"

      env {
        name  = "REDIS_HOST"
        value = "localhost"
      }
      env {
        name  = "MODEL_DIR"
        value = "/app/models"
      }
      env {
        name  = "SCORE_THRESHOLD_PATH"
        value = "/app/models/threshold.json"
      }
      env {
        name  = "KAFKA_BOOTSTRAP_SERVERS"
        value = "${azurerm_eventhub_namespace.main.name}.servicebus.windows.net:9093"
      }
    }

    # Redis sidecar: online feature store for /score. Containers in one app
    # share a network namespace, so the API reaches it at localhost:6379.
    # Ephemeral by design — features rebuild from the stream on restart.
    container {
      name   = "redis"
      image  = "docker.io/redis:7-alpine"
      cpu    = 0.25
      memory = "0.5Gi"
    }
  }

  ingress {
    external_enabled = true
    target_port      = 8000
    transport        = "auto"

    traffic_weight {
      percentage      = 100
      latest_revision = true
    }
  }

  lifecycle {
    ignore_changes = [
      # deploy.sh updates the image out-of-band after `az acr build`; avoid
      # terraform reverting it to the placeholder on subsequent plans.
      template[0].container[0].image,
    ]
  }
}
