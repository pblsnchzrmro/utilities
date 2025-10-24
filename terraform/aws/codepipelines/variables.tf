variable "bucket_name" {
  type = string
  default = "pblsnchzrmr"
}

variable "region" {
  type    = string
  default = "eu-west-1"
}

variable "notification_topic_arn" {
  description = "ARN of the SNS topic for pipeline errors notifications"
  type        = string
  default     = null
}

variable "environment" {
  type = string
  default = "dev"
}