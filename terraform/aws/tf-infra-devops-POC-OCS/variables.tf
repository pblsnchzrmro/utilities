#################
### VARIABLES ###
#################

variable "region" {
  type    = string
  default = "eu-west-1"
}

variable "devops_account_id" {
  default = "851725324220"
}

variable "github_code_repository_prefix" {
  type    = string
  default = "grupomediaset/terraform-euw1-mes-dd-snowflake-"
}

data "aws_caller_identity" "current" {}
output "account_id" {
  value = data.aws_caller_identity.current.account_id
}

variable "bucket_name" {
  type    = string
  default = "snowflake-cicd-pipeline"
}


##############
### LOCALS ###
##############

locals {
  descripcion = "SNOWFLAKE-DEVOPS"

  # Cuenta en la que se generan los recursos
  aws_account_mapping = {
    devops = "SNOWFLAKE_DEVOPS"
  }

  tags = {
    repo         = "github.com:grupomediaset/terraform-euw1-mes-dd-snowflake-devops"
    workspace    = terraform.workspace
    owner        = "Hiberus"
    iac          = "terraform"
    map-migrated = "d-server-00o0or1j9uiqin"
  }

  # Use case repositories
  repositories = [
    "maxmind",
    "kantar"
    # "sofia",
    # "marketingdata"
  ]
}
