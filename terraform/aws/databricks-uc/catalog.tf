resource "databricks_catalog" "environment" {
  provider = databricks.workspace

  name         = "${var.env_short}"
  comment      = "Managed by TF"
  storage_root = "s3://${aws_s3_bucket.uc_storage_bucket.bucket}/catalog"
  depends_on = [ databricks_external_location.some ]

  
#   depends_on = [databricks_metastore_assignment.primary]
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