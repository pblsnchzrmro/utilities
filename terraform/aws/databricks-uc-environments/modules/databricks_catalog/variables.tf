variable "name" {
  description = "Name of the catalog"
  type        = string
}

variable "storage_root" {
  description = "Cloud storage path for the catalog's managed tables (optional)"
  type        = string
  default     = null
}

variable "owner" {
  description = "Owner of the catalog (optional)"
  type        = string
  default     = null
}

variable "isolation_mode" {
  description = "Isolation mode: ISOLATED or OPEN (optional)"
  type        = string
  default     = null
}

variable "enable_predictive_optimization" {
  description = "ENABLE, DISABLE, or INHERIT predictive optimization (optional)"
  type        = string
  default     = null
}

variable "force_destroy" {
  description = "Whether to force delete the catalog even if it contains objects"
  type        = bool
  default     = false
}

variable "permissions" {
  description = "List of grants for the catalog"
  type = list(object({
    principal  = string
    privileges = list(string)
  }))
  default = []
}