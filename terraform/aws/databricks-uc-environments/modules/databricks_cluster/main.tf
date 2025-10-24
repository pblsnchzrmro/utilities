locals {
  use_autoscale    = var.autoscale != null && var.is_single_node != true
  use_num_workers  = var.num_workers != null && !local.use_autoscale && var.is_single_node != true
  use_fixed_config = local.use_autoscale || local.use_num_workers || var.is_single_node
}

resource "databricks_cluster" "this" {
  cluster_name                = var.cluster_name
  spark_version               = var.spark_version
  runtime_engine              = var.runtime_engine
  use_ml_runtime              = var.use_ml_runtime
  is_single_node              = var.is_single_node
  driver_node_type_id         = var.driver_node_type_id
  node_type_id                = var.node_type_id
  instance_pool_id            = var.instance_pool_id
  driver_instance_pool_id     = var.driver_instance_pool_id
  policy_id                   = var.policy_id
  apply_policy_default_values = var.apply_policy_default_values
  autotermination_minutes     = var.autotermination_minutes
  enable_elastic_disk         = var.enable_elastic_disk
  enable_local_disk_encryption = var.enable_local_disk_encryption
  kind                        = var.kind
  data_security_mode          = var.data_security_mode
  single_user_name            = var.single_user_name
  idempotency_token           = var.idempotency_token
  ssh_public_keys             = var.ssh_public_keys
  spark_env_vars              = var.spark_env_vars
  custom_tags                 = var.custom_tags
  spark_conf                  = var.spark_conf
  is_pinned                   = var.is_pinned
  no_wait                     = var.no_wait

  num_workers = local.use_num_workers ? var.num_workers : var.is_single_node ? 0 : null

  dynamic "autoscale" {
    for_each = local.use_autoscale ? [var.autoscale] : []
    content {
      min_workers = autoscale.value.min_workers
      max_workers = autoscale.value.max_workers
    }
  }
}
