resource "databricks_permission_assignment" "metastoreowner" {
  group_name  = "metastoreowner"
  permissions = ["USER"]
  provider    = databricks.workspace
}






# data "databricks_group" "metastoreowner" {
#   display_name = "metastoreowner"
# }

# resource "databricks_permission_assignment" "metastoreowner" {
#   principal_id = data.databricks_group.metastoreowner.id
#   permissions  = ["USER"]
#   provider     = databricks.workspace
# }









