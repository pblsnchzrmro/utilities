data "databricks_aws_unity_catalog_assume_role_policy" "assume_role_policy" {
  provider       = databricks.workspace
  aws_account_id = var.aws_account_id
  role_name      = local.databricks_uc_main_storage_role_name
  external_id    = module.databricks_storage_credential_main.external_id
}

data "databricks_aws_unity_catalog_policy" "bucket_access_policy" {
  provider       = databricks.workspace
  aws_account_id = var.aws_account_id
  bucket_name    = module.s3_uc_bucket.name
  kms_name       = var.uc_bucket_kms_key_id
  role_name      = local.databricks_uc_main_storage_role_name
}

