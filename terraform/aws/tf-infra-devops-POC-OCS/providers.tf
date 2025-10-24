provider "aws" {
  region  = var.region
  profile = "851725324220_AWSAdministratorAccess" 
  #local.aws_account_mapping[terraform.workspace]
# [msuser]
# "851725324220_AWSAdministratorAccess" 
  default_tags {
    tags = {
      repo         = "github.com:grupomediaset/terraform-euw1-mes-dd-snowflake-devops"
      workspace    = terraform.workspace
      owner        = "Desarrollo del dato"
      iac          = "terraform"
      map-migrated = "d-server-00o0or1j9uiqin"
    }
  }
}

