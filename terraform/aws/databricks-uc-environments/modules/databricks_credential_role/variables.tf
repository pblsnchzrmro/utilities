variable "role_name" {
  type        = string
  description = "IAM role name"
}

variable "credential_name" {
  type        = string
  description = "Databricks credential name"
}

variable "purpose" {
  type        = string
  description = "Credential purpose: SERVICE or STORAGE"
  validation {
    condition     = contains(["SERVICE", "STORAGE"], var.purpose)
    error_message = "purpose must be either SERVICE or STORAGE"
  }
}

variable "comments" {
  type    = string
  default = ""
}

variable "aws_account_id" {
  type        = string
  description = "AWS account ID"
}

variable "uc_master_role_arn" {
  type        = string
  description = "ARN of the Unity Catalog Master Role"
}

variable "databricks_external_id" {
  type        = string
  description = "External ID for assume role trust policy"
}

variable "custom_policy_json" {
  type        = string
  description = "IAM policy (JSON string)"
}

variable "tags" {
  type        = map(string)
  default     = {}
}