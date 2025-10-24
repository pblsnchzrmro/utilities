variable "name" {
  type = string
}

variable "codebuild_role_arn" {
  type = string
}

variable "s3_encryption_key_arn" {
  type = string
}

variable "image_name" {
  type = string
}

variable "environment_type" {
  type    = string
  default = "LINUX_CONTAINER"
}

variable "compute_type" {
  type    = string
  default = "BUILD_GENERAL1_SMALL"
}

variable "image_pull_credentials_type" {
  type = string
}

variable "build_timeout" {
  type    = number
  default = 60
}

variable "queued_timeout" {
  type    = number
  default = 480
}

variable "buildspec_name" {
  type = string
}

variable "logs_path" {
  type = string
}

variable "tags" {
  type    = map(any)
  default = {}
}

# variable "source_connection_arn" {
#   type = string
#   description = "The ARN of the CodeStar connection to GitHub"
# }

# variable "source_repo_url" {
#   type = string
#   description = "The GitHub repository URL"
# }
