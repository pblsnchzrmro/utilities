# resource "databricks_credential" "credential" {
#   name           = var.credential_name
#   purpose        = "STORAGE"
#   comment        = "Managed by TF"
#   force_destroy  = true
#   skip_validation = true

#   aws_iam_role {
#     role_arn = var.role_arn
#   }
# }


resource "databricks_credential" "credential" {
  name         = var.credential_name
  purpose      = var.purpose
  comment        = "Managed by TF"

  aws_iam_role {
    role_arn = var.role_arn
  }

  read_only       = var.read_only
  skip_validation = var.skip_validation
  force_destroy   = var.force_destroy
  force_update    = var.force_update
  owner          = var.owner != null ? var.owner : null
  isolation_mode = var.isolation_mode != null ? var.isolation_mode : null
}

resource "databricks_grants" "credential_grants" {
  count = length(var.permissions) == 0 ? 0 : 1

  credential = databricks_credential.credential.id

  dynamic "grant" {
    for_each = var.permissions
    content {
      principal  = grant.value.principal
      privileges = grant.value.privileges
    }
  }
}