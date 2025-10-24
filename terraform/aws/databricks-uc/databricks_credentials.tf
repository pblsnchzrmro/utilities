resource "databricks_storage_credential" "external" {
    provider = databricks.workspace
  name = aws_iam_role.uc_access_role.name
  aws_iam_role {
    role_arn = aws_iam_role.uc_access_role.arn
  }
  comment = "Managed by TF"
}