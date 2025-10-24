variable "name" {
  type        = string
  description = "Bucket name"
}

variable "common_tags" {
  type        = map(string)
  description = "Map of tags."
  default     = {}
}

variable "bucket_versioning_enabled" {
  type        = bool
  description = "Bucket versioning flag"
  default     = false
}

variable "sse_algorithm" {
  description = "El algoritmo de cifrado a usar: AES256 o aws:kms"
  type        = string
  default     = "AES256"
  validation {
    condition     = contains(["AES256", "aws:kms"], var.sse_algorithm)
    error_message = "sse_algorithm debe ser AES256 o aws:kms"
  }
}

variable "kms_key_id" {
  description = "ARN o ID de la clave de KMS a usar si se selecciona aws:kms"
  type        = string
  default     = ""
}

variable "bucket_policy_statements" {
  type        = list(string)
  description = "Bucket policy"
  default     = []
}

variable "public_policies_enabled" {
  type = bool
  default = false
}

variable "force_destroy" {
  type = bool
  default = false
  description = "Disable deletion protection"
}