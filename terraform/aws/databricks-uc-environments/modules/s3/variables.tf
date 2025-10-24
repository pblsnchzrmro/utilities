variable "name" {
  description = "Name of the S3 bucket"
  type        = string
}

variable "common_tags" {
  description = "Map of tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "bucket_versioning_enabled" {
  description = "Whether to enable versioning on the bucket"
  type        = bool
  default     = false
}

variable "sse_algorithm" {
  description = "Server-side encryption algorithm to use: AES256 or aws:kms"
  type        = string
  default     = "AES256"

  validation {
    condition     = contains(["AES256", "aws:kms"], var.sse_algorithm)
    error_message = "sse_algorithm must be either AES256 or aws:kms"
  }
}

variable "kms_key_id" {
  description = "KMS key ID or ARN to use if aws:kms is selected"
  type        = string
  default     = ""
}

variable "bucket_policy_statements" {
  description = "List of bucket policy documents to apply"
  type        = list(string)
  default     = []
}

variable "public_policies_enabled" {
  description = "Whether to allow public policies on the bucket"
  type        = bool
  default     = false
}

variable "force_destroy" {
  description = "Whether to allow force deletion of the bucket (even if not empty)"
  type        = bool
  default     = false
}