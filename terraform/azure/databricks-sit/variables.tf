variable "location" {
  type        = string
  description = "(Optional) The location for resource deployment"
  default     = "northeurope"
}

variable "environment" {
  type        = string
  description = "(Required) Three character environment name"

  validation {
    condition     = length(var.environment) <= 3
    error_message = "Err: Environment cannot be longer than three characters."
  }
}

variable "project" {
  type        = string
  description = "Project Name"
}

variable "user_server" {
  type        = string
  description = "description"
}

variable "password_server" {
  type        = string
  description = "description"
}