variable "vpc_id" {
  description = "The ID of the VPC"
  type        = string
}

variable "name" {
  description = "Name tag for the route table"
  type        = string
}

variable "nat_gateway_id" {
  description = "Optional NAT Gateway ID for private subnets"
  type        = string
  default     = null
}

variable "internet_gateway_id" {
  description = "Optional Internet Gateway ID for public subnets"
  type        = string
  default     = null
}

variable "vpc_endpoint_routes" {
  description = "Optional map of prefix list IDs to VPC endpoint IDs"
  type        = map(string)
  default     = {}
}

variable "tags" {
  description = "Tags to apply to the route table"
  type        = map(string)
  default     = {}
}
