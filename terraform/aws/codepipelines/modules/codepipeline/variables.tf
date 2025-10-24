#### CodePipeline Variables ####
variable "name" {
  type = string
}

variable "pipeline_type" {
  type = string
}

variable "role_arn" {
  type = string
}

variable "s3_location" {
  type = string
}

variable "branch_name" {
  type = string
}

variable "environment_name" {
  type    = string
  default = ""
}

variable "github_repository_id" {
  type = string
}

variable "region" {
  type    = string
  default = "eu-west-1"
}

variable "validate_build_project_name" {
  type = string
}

variable "plan_build_project_name" {
  type = string
}

variable "apply_build_project_name" {
  type = string
}

variable "source_connection_arn" {
  type = string
}

variable "approval_notification_arn" {
  type = string
}

variable "notification_topic_arn" {
  description = "ARN of the SNS topic for pipeline errors notifications"
  type        = string
  default     = null
}

variable "tags" {
  type    = map(any)
  default = {}
}