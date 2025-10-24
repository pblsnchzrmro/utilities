terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.17.0"
    }
  }
}

provider "databricks" {
  host  = "https://adb-6073501853274332.12.azuredatabricks.net"
  token = ""
}