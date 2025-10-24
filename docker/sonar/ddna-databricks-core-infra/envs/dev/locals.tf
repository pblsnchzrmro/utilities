locals {
  naming_base           = "${var.project_name}-${var.env}-${var.region}"
  naming_base_no_dash   = "${var.project_name}${var.env}${var.region}"
  naming_base_no_region = "${var.project_name}-${var.env}"

  az_aliases = {
    "eu-west-1a" = "AZ1"
    "eu-west-1b" = "AZ2"
    "eu-west-1c" = "AZ3"
  }

  subnet_custom_tags = {
    for az in var.azs :
    az => {
      Name = "processing${var.env} ${var.project_name} Private (WITH NAT) Subnet (${local.az_aliases[az]})"
    }
  }
  # TODO
  # databricks_root_bucket_policy = jsonencode({
  #   Version = "2012-10-17",
  #   Statement = [
  #     {
  #       Sid    = "GrantDatabricksAccess",
  #       Effect = "Allow",
  #       Principal = {
  #         AWS = "arn:aws:iam::414351767826:root"
  #       },
  #       Action = [
  #         "s3:GetObject",
  #         "s3:GetObjectVersion",
  #         "s3:PutObject",
  #         "s3:DeleteObject",
  #         "s3:ListBucket",
  #         "s3:GetBucketLocation"
  #       ],
  #       Resource = [
  #         "arn:aws:s3:::databricks-rootbucket-${local.naming_base}/*",
  #         "arn:aws:s3:::databricks-rootbucket-${local.naming_base}"
  #       ],
  #       Condition = {
  #         StringEquals = {
  #           "aws:PrincipalTag/DatabricksAccountId" = var.databricks_account_id
  #         }
  #       }
  #     }S
  #   ]
  # })

  common_tags = {
    Project = var.project_name
    Environment = var.env
    Repository = "ddna-databricks-core-infra"
    CostCenter = "UNIT DATA - PROJECT DATABRICKS CORE" 
  }
}