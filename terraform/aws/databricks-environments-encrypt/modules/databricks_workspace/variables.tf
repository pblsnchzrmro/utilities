# Core configuration
variable "databricks_account_id" {
  description = "The Databricks MWS account ID."
  type        = string
}

variable "region" {
  description = "The AWS region where the workspace will be deployed."
  type        = string
}

variable "workspace_name" {
  description = "The name of the Databricks workspace."
  type        = string
}

# IAM Role and Policy
variable "use_existing_cross_account_role" {
  description = "Whether to use an existing IAM role for Databricks cross-account access."
  type        = bool
  default     = false
}

variable "existing_cross_account_role_arn" {
  description = "ARN of an existing IAM role (used if use_existing_cross_account_role = true)."
  type        = string
  default     = null
}

variable "iam_role_name" {
  description = "Name of the IAM role to be created (ignored if using existing role)."
  type        = string
  default     = null
}

variable "iam_policy_name" {
  description = "Name of the IAM policy to be created (ignored if using existing role)."
  type        = string
  default     = null
}

# Databricks credentials
variable "credentials_name" {
  description = "Name of the Databricks credentials resource."
  type        = string
}

# Storage configuration
variable "bucket_name" {
  description = "The S3 bucket name used for workspace storage."
  type        = string
}

variable "storage_configuration_name" {
  description = "Name of the Databricks storage configuration resource."
  type        = string
}

# Network configuration
variable "network_name" {
  description = "Name of the Databricks network configuration."
  type        = string
}

variable "vpc_id" {
  description = "VPC ID to associate with the workspace."
  type        = string
}

variable "subnet_ids" {
  description = "List of subnet IDs to assign to the workspace."
  type        = list(string)
}

variable "security_group_ids" {
  description = "List of security group IDs to associate with the workspace."
  type        = list(string)
}

# Optional: Metastore assignment
variable "enable_metastore_assignment" {
  description = "Whether to assign a Unity Catalog metastore to the workspace."
  type        = bool
  default     = false
}

variable "metastore_id" {
  description = "The ID of the Unity Catalog metastore to assign."
  type        = string
  default     = null
}

# Optional: Customer-managed KMS for managed services
variable "enable_managed_services_key" {
  description = "Whether to configure a customer-managed KMS key for managed services encryption."
  type        = bool
  default     = false
}

variable "managed_services_key_arn" {
  description = "KMS key ARN for managed services encryption."
  type        = string
  default     = null
}

variable "managed_services_key_alias" {
  description = "Alias of the managed services KMS key."
  type        = string
  default     = null
}

# Optional: Customer-managed KMS for workspace storage
variable "enable_workspace_storage_key" {
  description = "Whether to configure a customer-managed KMS key for workspace storage encryption."
  type        = bool
  default     = false
}

variable "workspace_storage_key_arn" {
  description = "KMS key ARN for workspace storage encryption."
  type        = string
  default     = null
}

variable "workspace_storage_key_alias" {
  description = "Alias of the workspace storage KMS key."
  type        = string
  default     = null
}

# Optional: AWS resource tags
variable "tags" {
  description = "Tags to apply to AWS resources created in this module."
  type        = map(string)
  default     = {}
}
