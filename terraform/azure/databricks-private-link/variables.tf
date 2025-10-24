variable "no_public_ip" {
  type        = bool
  default     = true
  description = "If Secure Cluster Connectivity (No Public IP) should be enabled. Default: true"
}

variable "rglocation" {
  type        = string
  default     = "northeurope"
  description = "Location of resource group to create"
}

variable "dbfs_prefix" {
  type        = string
  default     = "dbfs"
  description = "Name prefix for DBFS Root Storage account"
}

variable "workspace_prefix" {
  type        = string
  default     = "adb"
  description = "Name prefix for Azure Databricks workspace"
}

variable "cidr" {
  type        = string
  default     = "10.179.0.0/20"
  description = "Network range for created VNet"
}

data "external" "me" {
  program = ["az", "account", "show", "--query", "user"]
}

locals {
  prefix = "adb-private-link-simple"
  dbfsname = "adbprivatelinksimpledbfs"
  tags = {
    Environment = "Demo"
    Owner       = lookup(data.external.me.result, "name")
  }
}