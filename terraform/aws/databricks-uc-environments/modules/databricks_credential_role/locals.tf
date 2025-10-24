locals {
  self_role_arn = format("arn:aws:iam::%s:role/%s", var.aws_account_id, var.role_name)

  assume_role_principals = [
    var.uc_master_role_arn,
    local.self_role_arn
  ]
}
