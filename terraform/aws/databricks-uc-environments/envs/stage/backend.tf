terraform {
  backend "s3" {
    bucket = "project-stage-tfstates"
    key    = "terraform.tfstate"
    region = "eu-west-1"
  }
}