# -----------------------------------------------------------------------------
# BUCKETS
# -----------------------------------------------------------------------------

module "s3_uc_bucket" {
  source                    = "../../modules/s3"
  name                      = "dd-ucbucket-${local.naming_base}"
  bucket_versioning_enabled = var.uc_bucket_versioning_enabled
  bucket_policy_statements  = []
  force_destroy             = false
  sse_algorithm             = var.uc_bucket_sse_algorithm
  kms_key_id                = var.uc_bucket_kms_key_id
  common_tags               = local.common_tags
}

# -----------------------------------------------------------------------------  
# Unity Catalog storage credential  
# -----------------------------------------------------------------------------

module "databricks_storage_credential_main" {
  source = "../../modules/databricks_credential"

  providers = {
    databricks = databricks.workspace
  }

  credential_name = "dd-${var.project_name}-${var.env}-${var.region}-uc-cred"
  role_arn        = "arn:aws:iam::${var.aws_account_id}:role/${local.databricks_uc_main_storage_role_name}"
  purpose         = "STORAGE"

  read_only       = false
  skip_validation = false
  force_destroy   = false
  force_update    = false
  isolation_mode  = "ISOLATION_MODE_ISOLATED"

  permissions = [
    {
      principal  = "pblsnchzrmr@gmail.com"
      privileges = ["ALL_PRIVILEGES"]
    }
  ]
}

module "iam_uc_role" {
  source              = "../../modules/iam_role"
  depends_on = [ module.databricks_storage_credential_main ]
  role_name           = local.databricks_uc_main_storage_role_name
  assume_role_policy  = data.databricks_aws_unity_catalog_assume_role_policy.assume_role_policy.json
  inline_policy_json  = data.databricks_aws_unity_catalog_policy.bucket_access_policy.json
  tags                = local.common_tags
}

# -----------------------------------------------------------------------------
# UNITY CATALOG EXTERNAL LOCATIONS
# -----------------------------------------------------------------------------

module "databricks_external_location_main" {
  source = "../../modules/databricks_external_location"

  providers = {
    databricks = databricks.workspace
  }

  name            = "${module.s3_uc_bucket.name}-external-location"
  url             = "s3://${module.s3_uc_bucket.id}/"
  credential_name = module.databricks_storage_credential_main.credential_name

  isolation_mode  = "ISOLATION_MODE_ISOLATED"
  skip_validation = false
  force_destroy   = true

  permissions = [
    {
      principal  = "pblsnchzrmr@gmail.com"
      privileges = ["ALL_PRIVILEGES"]
    }
  ]
}


# -----------------------------------------------------------------------------
# CATALOGS
# -----------------------------------------------------------------------------

module "databricks_catalog_environment" {
  source = "../../modules/databricks_catalog"

  providers = {
    databricks = databricks.workspace
  }

  name         = var.env
  storage_root = "s3://${module.s3_uc_bucket.name}/catalog"
  isolation_mode = "ISOLATED"
  force_destroy  = true
  enable_predictive_optimization = "INHERIT"

  permissions = [
    {
      principal  = "pblsnchzrmr@gmail.com"
      privileges = ["USE_CATALOG", "CREATE_SCHEMA"]
    }
  ]
}

#cluster


module "auto_cluster" {
  providers = {
    databricks = databricks.workspace
  }
  source         = "../../modules/databricks_cluster"
  cluster_name   = "autoscale-cluster"
  spark_version  = "13.3.x-scala2.12"
  node_type_id   = "i3.xlarge"
  autoscale = {
    min_workers = 1
    max_workers = 2
  }
}