variable "prefix" {
  description = "Prefijo para nombrar los recursos"
  type        = string
}



variable "tags" {
  description = "Etiquetas comunes aplicadas a todos los recursos"
  type        = map(string)
  default     = {}
}

variable "databricks_account_id" {
  type = string
}

variable "databricks_account_client_id" {
  type = string
}

variable "databricks_account_client_secret" {
  type = string
}

variable "aws_account_id" {
  type = string
}

variable "metastore_name" {
  type = string
}

variable "region" {
  type = string
}

variable "region_short" {
  type = string
}

variable "project" {
  type = string
}

variable "env_short" {
  type = string
}