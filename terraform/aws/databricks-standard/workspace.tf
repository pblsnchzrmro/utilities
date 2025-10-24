resource "databricks_mws_networks" "this" {
  account_id         = var.databricks_account_id
  network_name       = "${local.naming_base}-network"
  security_group_ids = [module.vpc.default_security_group_id]
  subnet_ids         = module.vpc.private_subnets
  vpc_id             = module.vpc.vpc_id
}

resource "databricks_mws_storage_configurations" "this" {
  account_id                 = var.databricks_account_id
  bucket_name                = aws_s3_bucket.root_storage_bucket.bucket
  storage_configuration_name = "${local.naming_base}-storage"
}

resource "databricks_mws_credentials" "this" {
  role_arn         = aws_iam_role.cross_account_role.arn
  credentials_name = "${local.naming_base}-creds"
}

resource "databricks_mws_workspaces" "this" {
  account_id     = var.databricks_account_id
  aws_region     = var.region
  workspace_name = local.naming_base

  credentials_id           = databricks_mws_credentials.this.credentials_id
  storage_configuration_id = databricks_mws_storage_configurations.this.storage_configuration_id
  network_id               = databricks_mws_networks.this.network_id

  token {
    comment = "Terraform"
  }
}

resource "databricks_metastore_assignment" "metastore" {
  metastore_id = data.databricks_metastore.metastore.metastore_id
  workspace_id = databricks_mws_workspaces.this.workspace_id
}

# resource "databricks_mws_customer_managed_keys" "managed_storage" {
#   account_id = var.databricks_account_id
#   aws_key_info {
#     key_arn   = data.aws_kms_alias.managed_storage.target_key_arn
#     key_alias = data.aws_kms_alias.managed_storage.name
#   }
#   use_cases = ["MANAGED_SERVICES"]
# }

# resource "databricks_mws_customer_managed_keys" "workspace_storage" {
#   account_id = var.databricks_account_id
#   aws_key_info {
#     key_arn   = data.aws_kms_alias.workspace_storage.target_key_arn
#     key_alias = data.aws_kms_alias.workspace_storage.name
#   }
#   use_cases = ["STORAGE"]
# }
