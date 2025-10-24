resource "databricks_catalog" "environment" {
  provider = databricks.workspace
  name         = "${var.env}"
  comment      = "Managed by TF"
  storage_root = "s3://${module.s3_uc_bucket.name}/catalog"
  depends_on = [ databricks_external_location.uc_storage_bucket ]
}

// Assign Permissions to the Databricks Catalog (optional)
resource "databricks_grants" "democatalog_permissions" {
  provider = databricks.workspace
  catalog = databricks_catalog.environment.name

  grant {
    principal  = "pblsnchzrmr@gmail.com"
    privileges = ["USE_CATALOG"]
  }
}