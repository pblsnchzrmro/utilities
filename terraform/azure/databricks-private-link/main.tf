# Azure Provider source and version being used
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">=3.43.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "=0.5.1"
    }
  }
}



resource "azurerm_resource_group" "this" {
  name     = "${local.prefix}-rg"
  location = var.rglocation
  tags     = local.tags
}

