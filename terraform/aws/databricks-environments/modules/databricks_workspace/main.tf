# --- IAM Role and Policy (optional or automatic) ---
data "databricks_aws_assume_role_policy" "this" {
  external_id = var.databricks_account_id
}

data "databricks_aws_crossaccount_policy" "this" {}

resource "aws_iam_role" "cross_account_role" {
  count              = var.use_existing_cross_account_role ? 0 : 1
  name               = var.iam_role_name
  assume_role_policy = data.databricks_aws_assume_role_policy.this.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "cross_account_policy" {
  count  = var.use_existing_cross_account_role ? 0 : 1
  name   = var.iam_policy_name
  role   = aws_iam_role.cross_account_role[0].id
  policy = data.databricks_aws_crossaccount_policy.this.json
}

# --- Credentials using auto or existing role ---
resource "databricks_mws_credentials" "workspace_credentials" {
  credentials_name = var.credentials_name
  role_arn         = var.use_existing_cross_account_role ? var.existing_cross_account_role_arn : aws_iam_role.cross_account_role[0].arn

}

# --- Storage configuration ---
resource "databricks_mws_storage_configurations" "workspace_storage" {
  account_id                 = var.databricks_account_id
  bucket_name                = var.bucket_name
  storage_configuration_name = var.storage_configuration_name
}

# --- Network configuration ---
resource "databricks_mws_networks" "workspace_network" {
  account_id         = var.databricks_account_id
  network_name       = var.network_name
  security_group_ids = var.security_group_ids
  subnet_ids         = var.subnet_ids
  vpc_id             = var.vpc_id
}

# --- Workspace creation ---
resource "databricks_mws_workspaces" "workspace" {
  account_id     = var.databricks_account_id
  aws_region     = var.region
  workspace_name = var.workspace_name

  credentials_id           = databricks_mws_credentials.workspace_credentials.credentials_id
  storage_configuration_id = databricks_mws_storage_configurations.workspace_storage.storage_configuration_id
  network_id               = databricks_mws_networks.workspace_network.network_id
  
}

# --- Metastore assignment (optional) ---
resource "databricks_metastore_assignment" "workspace_metastore" {
  count        = var.enable_metastore_assignment ? 1 : 0
  metastore_id = var.metastore_id
  workspace_id = databricks_mws_workspaces.workspace.workspace_id

}

# --- Managed Services Key (optional) ---
resource "databricks_mws_customer_managed_keys" "managed_services_key" {
  count      = var.enable_managed_services_key ? 1 : 0
  account_id = var.databricks_account_id

  aws_key_info {
    key_arn   = var.managed_services_key_arn
    key_alias = var.managed_services_key_alias
  }

  use_cases = ["MANAGED_SERVICES"]
}

# --- Workspace Storage Key (optional) ---
resource "databricks_mws_customer_managed_keys" "workspace_storage_key" {
  count      = var.enable_workspace_storage_key ? 1 : 0
  account_id = var.databricks_account_id

  aws_key_info {
    key_arn   = var.workspace_storage_key_arn
    key_alias = var.workspace_storage_key_alias
  }

  use_cases = ["STORAGE"]
}
