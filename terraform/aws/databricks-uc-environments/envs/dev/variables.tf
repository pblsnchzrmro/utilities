# ------------------------------------------------------------------------------
# Project and environment configuration
# ------------------------------------------------------------------------------

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "env" {
  description = "Environment identifier (e.g. dev, stage, prod)"
  type        = string
}

variable "region" {
  description = "AWS region where resources will be created"
  type        = string
}

# ------------------------------------------------------------------------------
# AWS and Databricks account configuration
# ------------------------------------------------------------------------------

variable "aws_account_id" {
  description = "AWS account ID where the infrastructure will be deployed"
  type        = string
}

variable "databricks_account_id" {
  description = "Databricks account ID"
  type        = string
}

# ------------------------------------------------------------------------------
# S3 bucket configuration for Unity Catalog
# ------------------------------------------------------------------------------

variable "uc_bucket_versioning_enabled" {
  description = "Enable versioning for the Unity Catalog bucket"
  type        = bool
}

variable "uc_bucket_sse_algorithm" {
  description = "Server-side encryption algorithm for the bucket (AES256 or aws:kms)"
  type        = string
}

variable "uc_bucket_kms_key_id" {
  description = "KMS key ID or ARN to use if aws:kms is selected"
  type        = string
}

variable "bucket_policy_statements" {
  description = "List of policy documents to attach to the bucket"
  type        = list(string)
  default     = []
}

# ------------------------------------------------------------------------------
# KMS configuration for storage credentials
# ------------------------------------------------------------------------------

variable "enable_kms" {
  description = "Whether to enable KMS permissions for encrypted storage"
  type        = bool
}