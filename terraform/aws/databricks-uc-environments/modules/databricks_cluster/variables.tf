variable "cluster_name" {
  description = "(Optional) Cluster name. Defaults to empty string if not set."
  type        = string
  default     = ""
}

variable "spark_version" {
  description = "(Required) Databricks Runtime version."
  type        = string
}

variable "runtime_engine" {
  description = "(Optional) Runtime engine: PHOTON or STANDARD."
  type        = string
  default     = null
}

variable "use_ml_runtime" {
  description = "(Optional) Whether to use ML runtime. Only used with 'kind'."
  type        = bool
  default     = false
}

variable "is_single_node" {
  description = "(Optional) Set to true to run as single node cluster. Implies num_workers = 0."
  type        = bool
  default     = false
}

variable "num_workers" {
  description = "(Optional) Number of worker nodes. Cannot be set with autoscale or single node."
  type        = number
  default     = null
}

variable "autoscale" {
  description = "(Optional) Autoscale settings. Cannot be set with num_workers or single node."
  type = object({
    min_workers = number
    max_workers = number
  })
  default = null
}

variable "node_type_id" {
  description = "(Required unless instance_pool_id is given) Node type for workers."
  type        = string
  default     = null
}

variable "driver_node_type_id" {
  description = "(Optional) Node type for driver. Defaults to node_type_id."
  type        = string
  default     = null
}

variable "instance_pool_id" {
  description = "(Optional) Instance pool for worker nodes. Required if node_type_id is not set."
  type        = string
  default     = null
}

variable "driver_instance_pool_id" {
  description = "(Optional) Instance pool for driver node."
  type        = string
  default     = null
}

variable "policy_id" {
  description = "(Optional) Cluster policy ID."
  type        = string
  default     = null
}

variable "apply_policy_default_values" {
  description = "(Optional) Use policy defaults for missing attributes."
  type        = bool
  default     = false
}

variable "autotermination_minutes" {
  description = "(Optional) Idle time before termination. 0 disables. Recommended default is 60."
  type        = number
  default     = 60
}

variable "enable_elastic_disk" {
  description = "(Optional) Enable autoscaling of local disk (EBS)."
  type        = bool
  default     = false
}

variable "enable_local_disk_encryption" {
  description = "(Optional) Enable local disk encryption. May impact performance."
  type        = bool
  default     = false
}

variable "kind" {
  description = "(Optional) Kind of compute (e.g., CLASSIC_PREVIEW)."
  type        = string
  default     = null
}

variable "data_security_mode" {
  description = "(Optional) Security mode. Required for Unity Catalog (SINGLE_USER or USER_ISOLATION)."
  type        = string
  default     = null
}

variable "single_user_name" {
  description = "(Optional) Username/group for SINGLE_USER mode."
  type        = string
  default     = null
}

variable "idempotency_token" {
  description = "(Optional) Token to ensure idempotent creation. Max 64 chars."
  type        = string
  default     = null
}

variable "ssh_public_keys" {
  description = "(Optional) List of SSH public keys for access to nodes."
  type        = list(string)
  default     = []
}

variable "spark_env_vars" {
  description = "(Optional) Spark environment variables."
  type        = map(string)
  default     = {}
}

variable "custom_tags" {
  description = "(Optional) Custom tags for EC2 and EBS resources."
  type        = map(string)
  default     = {}
}

variable "spark_conf" {
  description = "(Optional) Spark configuration overrides."
  type        = map(string)
  default     = {}
}

variable "is_pinned" {
  description = "(Optional) Whether the cluster is pinned. Admin-only."
  type        = bool
  default     = false
}

variable "no_wait" {
  description = "(Optional) Whether to skip waiting for cluster to be running."
  type        = bool
  default     = false
}
