resource "databricks_external_location" "some" {
  provider = databricks.workspace
  name            = "external-location-${local.naming_base}"
  url             = "s3://${aws_s3_bucket.uc_storage_bucket.id}/"
  credential_name = databricks_storage_credential.external.id
  force_destroy = true
  comment         = "Managed by TF"
}