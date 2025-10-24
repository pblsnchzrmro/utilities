# data "terraform_remote_state" "integracion-snowflake-dev" {
#   backend = "s3"
#   config = {
#     bucket  = "terraform-remote-state-vault"
#     key     = "DD/integracion-snowflake/dev/terraform.tfstate"
#     region  = "eu-west-1"
#     profile = "msuser"
#   }
# }
# data "terraform_remote_state" "integracion-snowflake-pre" {
#   backend = "s3"
#   config = {
#     bucket  = "terraform-remote-state-vault"
#     key     = "DD/integracion-snowflake/pre/terraform.tfstate"
#     region  = "eu-west-1"
#     profile = "msuser"
#   }
# }
# data "terraform_remote_state" "integracion-snowflake-pro" {
#   backend = "s3"
#   config = {
#     bucket  = "terraform-remote-state-vault"
#     key     = "DD/integracion-snowflake/pro/terraform.tfstate"
#     region  = "eu-west-1"
#     profile = "msuser"
#   }
# }
# data "terraform_remote_state" "automation-devops-infra" {
#   backend = "s3"
#   config = {
#     bucket  = "terraform-remote-state-vault"
#     key     = "DD/automation-devops-infra/devops/terraform.tfstate"
#     region  = "eu-west-1"
#     profile = "msuser"
#   }
# }