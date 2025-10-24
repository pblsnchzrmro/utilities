# COMMON
variable "project_name" {
  type        = string
  description = "Name of the project"
}

variable "env" {
  type        = string
  description = "Environment"
}

variable "region" {
  type        = string
  description = "Project region"
}

#BUCKET
variable "root_bucket_versioning_enabled" {
  type        = string
  description = "Root bucket versioning"
}

variable "root_bucket_sse_algorithm" {
  description = "Root bucket see algorithm"
  type        = string
  default     = "AES256"
}

variable "root_bucket_kms_key_id" {
  description = "KMS KEY"
  type        = string
  default     = ""
}

variable "bucket_policy_statements" {
  type        = list(string)
  description = "Bucket policies"
  default     = []
}

#VNET

variable "vpc_id" {
  type        = string
  description = "ID of the existing VPC"
}

#SUBNETS
variable "route_table_id" {
  type        = string
}

variable "common_tags" {
  type        = map(string)
  default     = {}
}

variable "azs" {
  type    = list(string)
}

variable "subnet_cidrs" {
  type    = list(string)
}

#SECURITY GROUP

variable "ingress_rules" {
  description = "List of security group ingress rules"
  type = list(object({
    from_port                = number
    to_port                  = number
    protocol                 = string
    cidr_blocks              = optional(list(string))
    ipv6_cidr_blocks         = optional(list(string))
    description              = optional(string)
    source_security_group_id = optional(string)
  }))
  default = []
}

variable "egress_rules" {
  description = "List of security group egress rules"
  type = list(object({
    from_port                = number
    to_port                  = number
    protocol                 = string
    cidr_blocks              = optional(list(string))
    ipv6_cidr_blocks         = optional(list(string))
    description              = optional(string)
    source_security_group_id = optional(string)
  }))
  default = []
}

#TODO Use OCS databricks account id
# variable "databricks_account_id" {
#   type        = string
#   description = "Databricks Account ID"
# }