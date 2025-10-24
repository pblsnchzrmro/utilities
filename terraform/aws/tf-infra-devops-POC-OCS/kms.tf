# data "aws_iam_policy_document" "kms_key_policy_doc" {
#   policy_id = "key_mlops_policy"
#   statement {
#     sid    = "Enable IAM User Permissions"
#     effect = "Allow"

#     principals {
#       identifiers = [
#         "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root",
#         "arn:aws:iam::${data.terraform_remote_state.integracion-snowflake-dev.outputs.account_id}:root",
#         "arn:aws:iam::${data.terraform_remote_state.integracion-snowflake-pre.outputs.account_id}:root",
#         "arn:aws:iam::${data.terraform_remote_state.integracion-snowflake-pro.outputs.account_id}:root"
#       ]
#       type = "AWS"
#     }
#     actions   = ["kms:*"]
#     resources = ["*"]
#   }
# }
# resource "aws_kms_key" "key_mlops" {
#   description              = "KMS for Cross-Accounts deployments"
#   customer_master_key_spec = "SYMMETRIC_DEFAULT"
#   deletion_window_in_days  = 30
#   enable_key_rotation      = false
#   policy                   = data.aws_iam_policy_document.kms_key_policy_doc.json
# }
# resource "aws_kms_alias" "key_mlops_alias" {
#   name          = "alias/cross-account"
#   target_key_id = aws_kms_key.key_mlops.key_id
# }