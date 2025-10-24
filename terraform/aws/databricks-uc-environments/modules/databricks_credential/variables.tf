variable "credential_name" {
  description = "Name of the Unity Catalog credential"
  type        = string
}

variable "role_arn" {
  description = "ARN of the IAM role to register as a credential"
  type        = string
}

variable "purpose" {
  description = "Purpose of the credential: STORAGE or SERVICE"
  type        = string
  default     = "STORAGE"

  validation {
    condition     = contains(["STORAGE", "SERVICE"], var.purpose)
    error_message = "purpose must be either STORAGE or SERVICE"
  }
}

variable "owner" {
  description = "Optional owner of the credential"
  type        = string
  default     = null
}

variable "read_only" {
  description = "Whether the credential is read-only (only for STORAGE)"
  type        = bool
  default     = false
}

variable "skip_validation" {
  description = "Skip validation when creating the credential"
  type        = bool
  default     = false
}

variable "force_destroy" {
  description = "Force destroy the credential even if it's in use"
  type        = bool
  default     = false
}

variable "force_update" {
  description = "Force update the credential even if it has dependents"
  type        = bool
  default     = false
}

variable "isolation_mode" {
  description = "Isolation mode: ISOLATION_MODE_ISOLATED or ISOLATION_MODE_OPEN"
  type        = string
  default     = null
}

variable "permissions" {
  description = "List of grants for the credential"
  type = list(object({
    principal  = string
    privileges = list(string)
  }))
  default = []
}
