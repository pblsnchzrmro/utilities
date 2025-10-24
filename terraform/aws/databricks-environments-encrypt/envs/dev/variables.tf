variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "env" {
  description = "Deployment environment (e.g. dev, staging, prod)"
  type        = string
}

variable "region" {
  description = "AWS region where resources are deployed"
  type        = string
}

variable "vpc_id" {
  description = "ID of the VPC to use for subnets and security groups"
  type        = string
}

variable "igw_id" {
  description = "Internet Gateway ID for public route table"
  type        = string
}

variable "private_route_table_id" {
  description = "Route table ID for private subnets"
  type        = string
  default     = null
}

variable "public_route_table_id" {
  description = "Route table ID for public subnets"
  type        = string
  default     = null
}


variable "private_subnet_cidrs" {
  description = "Map of CIDRs for private subnets by AZ"
  type        = map(string)
}

variable "public_subnet_cidrs" {
  description = "Map of CIDRs for public subnets by AZ"
  type        = map(string)
  default     = {}
}

variable "common_tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "private_custom_tags" {
  description = "Custom tags for private subnets"
  type        = map(map(string))
  default     = {}
}

variable "public_custom_tags" {
  description = "Custom tags for public subnets"
  type        = map(map(string))
  default     = {}
}

variable "vpc_endpoint_routes" {
  description = "Map of prefix list IDs to VPC endpoint IDs (used in route tables)"
  type        = map(string)
  default     = {}
}

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

variable "root_bucket_versioning_enabled" {
  description = "Whether versioning should be enabled on the root bucket"
  type        = string
}

variable "sse_algorithm" {
  description = "Encryption algorithm: AES256 or aws:kms"
  type        = string
  default     = "AES256"
  validation {
    condition     = contains(["AES256", "aws:kms"], var.sse_algorithm)
    error_message = "sse_algorithm must be AES256 or aws:kms"
  }
}

variable "kms_key_id" {
  description = "KMS Key ID or ARN if aws:kms is selected"
  type        = string
  default     = ""
}

variable "bucket_policy_statements" {
  description = "List of IAM policy statements for the root bucket"
  type        = list(string)
  default     = []
}

# Optional: if you later integrate with Databricks Account Console (MWS)
# variable "databricks_account_id" {
#   description = "Databricks Account ID (for MWS deployments)"
#   type        = string
# }



###olyvpc###
variable "cidr_block" {
  description = "CIDR base para la VPC"
  type        = string
  default     = "10.0.0.0/22" # para que puedas dividir en /24 sin problemas
}

#databricks account

variable "databricks_account_id" {
  type = string
}

variable "databricks_account_client_id" {
  type = string
}

variable "databricks_account_client_secret" {
  type = string
}

variable "metastore_name" {
  type = string
}


variable "root_bucket_sse_algorithm" {
  description = "Server-side encryption algorithm for the root bucket (AES256 or aws:kms)"
  type        = string
  default     = "AES256"
}

variable "root_bucket_kms_key_id" {
  description = "KMS key ID or ARN to use if aws:kms is selected for the root bucket"
  type        = string
  default     = ""
}