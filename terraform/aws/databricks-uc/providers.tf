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

    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
  }
}


# provider "databricks" {
#   alias         = "mws"
#   host          = "https://accounts.cloud.databricks.com"
# }


provider "databricks" {
  alias         = "mws"
  host          = "https://accounts.cloud.databricks.com"
  client_id     = ""
  client_secret = ""
  account_id    = "ac4f2be4-cedd-4d6b-a6da-ebd59b826422"
}

provider "databricks" {
  alias         = "workspace"
  host          = "https://dbc-0c6991e0-73c2.cloud.databricks.com/"
  client_id     = ""
  client_secret = ""
  account_id    = "ac4f2be4-cedd-4d6b-a6da-ebd59b826422"
}