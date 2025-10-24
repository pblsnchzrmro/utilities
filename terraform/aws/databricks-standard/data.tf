# Datos requeridos por Databricks en AWS

data "databricks_aws_assume_role_policy" "this" {
  external_id = var.databricks_account_id
}

data "databricks_aws_crossaccount_policy" "this" {}

data "aws_availability_zones" "available" {}

data "databricks_aws_bucket_policy" "this" {
  bucket = aws_s3_bucket.root_storage_bucket.bucket
}

data "databricks_metastore" "metastore" {
  name = var.metastore_name
}

# data "aws_kms_alias" "managed_storage" {
#   name = "alias/bucket-encryption"
# }

# data "aws_kms_alias" "workspace_storage" {
#   name = "alias/bucket-encryption"
# }
