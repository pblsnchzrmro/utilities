# data "databricks_aws_assume_role_policy" "this" {
#   external_id = var.databricks_account_id
# }

# output "databricks_aws_assume_role_policy" {
#   value = databricks_aws_assume_role_policy.value
# }

# data "databricks_aws_crossaccount_policy" "this" {}

# resource "aws_iam_role" "cross_account_role" {
#   name               = "iam-role-${local.naming_base_no_region}-crossaccount"
#   assume_role_policy = data.databricks_aws_assume_role_policy.this.json
#   tags               = local.common_tags
# }

# resource "aws_iam_role_policy" "this" {
#   name   = "${local.naming_base_no_region}-policy"
#   role   = aws_iam_role.cross_account_role.id
#   policy = data.databricks_aws_crossaccount_policy.this.json
# }