terraform {
  backend "s3" {
    bucket = "project-prod-tfstates"
    key    = "terraform.tfstate"
    region = "eu-west-1"
  }
}