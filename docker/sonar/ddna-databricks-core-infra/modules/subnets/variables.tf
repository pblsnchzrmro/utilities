variable "vpc_id" {
  type = string
}

variable "azs" {
  type = list(string)
}

variable "subnet_cidrs" {
  type = list(string)
}

variable "route_table_id" {
  type = string
}

variable "common_tags" {
  type = map(string)
}

variable "custom_tags" {
  type = map(map(string))
  description = "Mapa con tags personalizados por AZ"
}
