resource "databricks_catalog" "catalog" {
  name         = var.name
  comment      = "Managed by TF"
  force_destroy = var.force_destroy

  storage_root = var.storage_root != null ? var.storage_root : null
  owner         = var.owner != null ? var.owner : null
  isolation_mode = var.isolation_mode != null ? var.isolation_mode : null
  enable_predictive_optimization = var.enable_predictive_optimization != null ? var.enable_predictive_optimization : null
}

resource "databricks_grants" "catalog_grants" {
  count = length(var.permissions) == 0 ? 0 : 1

  catalog = databricks_catalog.catalog.name

  dynamic "grant" {
    for_each = var.permissions
    content {
      principal  = grant.value.principal
      privileges = grant.value.privileges
    }
  }
}