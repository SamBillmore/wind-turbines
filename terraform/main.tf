terraform {
  required_version = "~>1.11.4"
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

# Configure the Databricks Provider

provider "databricks" {
  alias         = "workspace"
  host          = var.host
  client_id     = var.clientId
  client_secret = var.clientSecret
}

# Reference to manually created catalog with Default Storage

data "databricks_catalog" "wind_turbines" {
  name = "wind_turbines"
}

# Reference to default Serverless Starter Warehouse

data "databricks_sql_warehouse" "starter_warehouse" {
  name = "Serverless Starter Warehouse"
}
