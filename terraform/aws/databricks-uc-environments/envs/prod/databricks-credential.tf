resource "databricks_storage_credential" "credential_uc_bucket" {
  provider = databricks.workspace
  name = module.databricks_storage_credential.role_name
  aws_iam_role {
    role_arn = module.databricks_storage_credential.role_arn
  }
  depends_on = [ module.databricks_storage_credential ]
  comment = "Managed by TF" 
}