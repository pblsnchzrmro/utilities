variable "vpc_id" {
  description = "ID of the VPC where subnets will be created"
  type        = string
}

variable "subnet_cidrs" {
  description = "Map of private subnet CIDRs by availability zone"
  type        = map(string)
}

variable "public_subnet_cidrs" {
  description = "Map of public subnet CIDRs by availability zone"
  type        = map(string)
  default     = {}
}

variable "route_table_id" {
  description = "Route table ID to associate with private subnets"
  type        = string
}

variable "public_route_table_id" {
  description = "Route table ID to associate with public subnets"
  type        = string
  default     = null
}

variable "common_tags" {
  description = "Common tags applied to all subnets"
  type        = map(string)
}

variable "private_custom_tags" {
  description = "Custom tags per private subnet (keyed by AZ)"
  type        = map(map(string))
}

variable "public_custom_tags" {
  description = "Custom tags per public subnet (keyed by AZ)"
  type        = map(map(string))
  default     = {}
}