resource "databricks_external_location" "external_location" {
  name            = var.name
  url             = var.url
  credential_name = var.credential_name
  comment         = "Managed by TF"
  
  skip_validation = var.skip_validation
  force_destroy   = var.force_destroy

  owner           = var.owner != null ? var.owner : null
  isolation_mode  = var.isolation_mode != null ? var.isolation_mode : null
}


resource "databricks_grants" "external_location_grants" {
  count = length(var.permissions) == 0 ? 0 : 1

  external_location = databricks_external_location.external_location.id

  dynamic "grant" {
    for_each = var.permissions
    content {
      principal  = grant.value.principal
      privileges = grant.value.privileges
    }
  }
}