terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = ">=1.13.0"
    }

    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}


provider "databricks" {
  # alias         = "mws"
  host          = "https://accounts.cloud.databricks.com"
  client_id     = var.databricks_account_client_id
  client_secret = var.databricks_account_client_secret
  account_id    = var.databricks_account_id
  auth_type = "oauth-m2m"
}