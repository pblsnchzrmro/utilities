terraform {
  backend "s3" {
    bucket = "mi-bucket-tfstate"
    key    = "stage/terraform.tfstate"
    region = "eu-west-1"
  }
}