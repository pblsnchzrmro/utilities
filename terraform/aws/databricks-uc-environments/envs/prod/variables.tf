variable "aws_account_id" {
  type = string
  description = "AWS account id"
}

variable "AWS_ACCESS_KEY" {
  type = string
}

variable "AWS_SECRET_KEY" {
  type = string
}

variable "databricks_account_id" {
  type = string
  description = "Databricks account id"
}

variable "project_name" {
  type        = string
  description = "Name of the project"
}

variable "env" {
  type        = string
  description = "Environment"
}

variable "region" {
  type        = string
  description = "Project region"
}

variable "root_bucket_versioning_enabled" {
  type        = string
  description = "Root bucket versioning"
}

variable "bucket_policy_statements" {
  type        = list(string)
  description = "Bucket policies"
  default     = []
}

variable "s3_bucket_arns" {
  description = "List of S3 bucket ARNs to grant access to"
  type        = list(string)
  default     = []
}
