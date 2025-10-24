module "s3_uc_bucket" {
  source                    = "../../modules/s3"
  name                      = "databricks-ucbucket-${local.naming_base}"
  bucket_versioning_enabled = true
  force_destroy             = false
  public_policies_enabled   = false
  bucket_policy_statements  = []
  common_tags               = local.common_tags
}

module "databricks_storage_credential" {
  source               = "../../modules/uc-iam-roles"
  role_name            = "${local.naming_base}-uc-access"
  bucket_arn           = module.s3_uc_bucket.arn
  aws_account_id       = var.aws_account_id
  databricks_account_id = var.databricks_account_id
  uc_master_role_arn   = "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
  tags                 = local.common_tags
}

