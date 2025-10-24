variable "name" {
  description = "Name of the External Location (must be unique in metastore)"
  type        = string
}

variable "url" {
  description = "URL to the cloud storage path (e.g. s3://bucket-name/)"
  type        = string
}

variable "credential_name" {
  description = "Name of the storage credential to use"
  type        = string
}

variable "owner" {
  description = "Optional owner of the external location"
  type        = string
  default     = null
}

variable "comment" {
  description = "Optional description"
  type        = string
  default     = "Managed by TF"
}

variable "skip_validation" {
  description = "Whether to skip validation on creation"
  type        = bool
  default     = false
}

variable "force_destroy" {
  description = "Whether to force destroy the external location"
  type        = bool
  default     = true
}

variable "isolation_mode" {
  description = "Optional isolation mode: ISOLATION_MODE_ISOLATED or ISOLATION_MODE_OPEN"
  type        = string
  default     = null
}

variable "permissions" {
  description = "List of grants to apply"
  type = list(object({
    principal  = string
    privileges = list(string)
  }))
  default = []
}
