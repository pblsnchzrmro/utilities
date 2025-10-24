data "databricks_user" "pblsnchzrmr" {
  # provider = databricks.mws
  user_name = "pblsnchzrmr@gmail.com"
}


resource "databricks_mws_permission_assignment" "pblsnchzrmr_workspace_access" {
  # provider = databricks.mws
  workspace_id = module.databricks_workspace.workspace_id
  principal_id = data.databricks_user.pblsnchzrmr.id
  permissions  = ["ADMIN"]
  depends_on = [ module.databricks_workspace ]
}