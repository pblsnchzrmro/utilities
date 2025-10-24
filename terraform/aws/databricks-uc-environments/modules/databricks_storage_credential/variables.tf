variable "role_name" {
  type = string
}

variable "bucket_arn" {
  type = string
}

variable "aws_account_id" {
  type = string
}

variable "databricks_account_id" {
  type = string
}

variable "uc_master_role_arn" {
  description = "The Unity Catalog Master Role ARN provided by Databricks"
  type        = string
}

variable "tags" {
  type    = map(string)
  default = {}
}

variable "enable_kms" {
  description = "Enable KMS permissions for encrypted storage"
  type        = bool
  default     = false
}

variable "kms_key_arn" {
  description = "ARN of the KMS key if encryption is enabled"
  type        = string
  default     = ""
}

variable "credential_name" {
  type        = string
  description = "Name of the Unity Catalog credential"
}

variable "comments" {
  type        = string
  default     = ""
  description = "Optional comment for the credential"
}

variable "storage_credential_permissions" {
  type = list(object({
    principal  = string
    privileges = list(string)
  }))
  default = []
  description = "Lista de grants para el storage credential (principal y privilegios)"
}
