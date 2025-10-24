terraform {
  backend "s3" {
    bucket = "ddna-databricks-core-infra-tfstate-dev-eu-west-1"
    key    = "dev.tfstate"
    region = "eu-west-1"
  }
}