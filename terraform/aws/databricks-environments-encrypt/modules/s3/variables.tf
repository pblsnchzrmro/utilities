variable "name" {
  description = "Name of the S3 bucket."
  type        = string
}

variable "common_tags" {
  description = "Key-value map of tags to assign to the bucket."
  type        = map(string)
  default     = {}
}

variable "bucket_versioning_enabled" {
  description = "Whether to enable versioning on the bucket."
  type        = bool
  default     = false
}

variable "sse_algorithm" {
  description = "Server-side encryption algorithm to use (AES256 or aws:kms)."
  type        = string
  default     = "AES256"

  validation {
    condition     = contains(["AES256", "aws:kms"], var.sse_algorithm)
    error_message = "sse_algorithm must be either 'AES256' or 'aws:kms'."
  }
}

variable "kms_key_id" {
  description = "ARN or ID of the KMS key to use if 'aws:kms' is selected."
  type        = string
  default     = ""
}

variable "bucket_policy_statements" {
  description = "List of bucket policy statements (JSON strings)."
  type        = list(string)
  default     = []
}

variable "public_policies_enabled" {
  description = "Whether to allow public access policies."
  type        = bool
  default     = false
}

variable "force_destroy" {
  description = "Whether to allow the bucket to be destroyed even if it contains objects."
  type        = bool
  default     = false
}