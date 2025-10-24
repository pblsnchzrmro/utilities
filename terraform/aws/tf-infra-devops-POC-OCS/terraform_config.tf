terraform {
  required_version = "1.5.5"

  backend "s3" {
    bucket               = "terraform-remote-state-vault"
    workspace_key_prefix = "DD/snowflake-devops-infra"
    key                  = "terraform.tfstate"
    region               = "eu-west-1"
    dynamodb_table       = "terraform_statelock"
  }

  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = "2.1.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.39.0"
    }
  }
}


