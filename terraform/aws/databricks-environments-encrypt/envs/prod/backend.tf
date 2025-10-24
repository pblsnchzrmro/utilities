terraform {
  backend "s3" {
    bucket = "mi-bucket-tfstate"
    key    = "pro/terraform.tfstate"
    region = "eu-west-1"
  }
}