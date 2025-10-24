data "databricks_metastore" "metastore" {
    provider = databricks.mws
  name = "ireland-metastore"
}