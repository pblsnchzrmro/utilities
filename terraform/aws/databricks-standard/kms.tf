# locals {
#   cmk_admin_value = "arn:aws:iam::354861872002:root"
# }

# resource "aws_kms_key" "workspace_storage" {
#   description = "KMS key for databricks workspace storage"
#   policy = jsonencode({
#     Version : "2012-10-17",
#     "Id" : "key-policy-workspace-storage",
#     Statement : [
#       {
#         "Sid" : "Enable IAM User Permissions",
#         "Effect" : "Allow",
#         "Principal" : {
#           "AWS" : [local.cmk_admin_value]
#         },
#         "Action" : "kms:*",
#         "Resource" : "*"
#       },
#       {
#         "Sid" : "Allow Databricks to use KMS key for DBFS",
#         "Effect" : "Allow",
#         "Principal" : {
#           "AWS" : "arn:aws:iam::414351767826:root"
#         },
#         "Action" : [
#           "kms:Encrypt",
#           "kms:Decrypt",
#           "kms:ReEncrypt*",
#           "kms:GenerateDataKey*",
#           "kms:DescribeKey"
#         ],
#         "Resource" : "*",
#         "Condition" : {
#           "StringEquals" : {
#             "aws:PrincipalTag/DatabricksAccountId" : [var.databricks_account_id]
#           }
#         }
#       },
#       {
#         "Sid" : "Allow Databricks to use KMS key for EBS",
#         "Effect" : "Allow",
#         "Principal" : {
#           "AWS" : aws_iam_role.cross_account_role.arn
#         },
#         "Action" : [
#           "kms:Decrypt",
#           "kms:GenerateDataKey*",
#           "kms:CreateGrant",
#           "kms:DescribeKey"
#         ],
#         "Resource" : "*",
#         "Condition" : {
#           "ForAnyValue:StringLike" : {
#             "kms:ViaService" : "ec2.*.amazonaws.com"
#           }
#         }
#       }
#     ]
#   })
#   depends_on = [aws_iam_role.cross_account_role]

#   tags = {
#     Name    = "${var.prefix}-workspace-storage-key"
#     Project = var.prefix
#   }
# }


# resource "aws_kms_alias" "workspace_storage_key_alias" {
#   name          = "alias/${var.prefix}-workspace-storage-key"
#   target_key_id = aws_kms_key.workspace_storage.id
# }

# # CMK for Managed Storage

# resource "aws_kms_key" "managed_storage" {
#   description = "KMS key for managed storage"
#   policy = jsonencode({ Version : "2012-10-17",
#     "Id" : "key-policy-managed-storage",
#     Statement : [
#       {
#         "Sid" : "Enable IAM User Permissions",
#         "Effect" : "Allow",
#         "Principal" : {
#           "AWS" : [local.cmk_admin_value]
#         },
#         "Action" : "kms:*",
#         "Resource" : "*"
#       },
#       {
#         "Sid" : "Allow Databricks to use KMS key for managed services in the control plane",
#         "Effect" : "Allow",
#         "Principal" : {
#           "AWS" : "arn:aws:iam::414351767826:root"
#         },
#         "Action" : [
#           "kms:Encrypt",
#           "kms:Decrypt"
#         ],
#         "Resource" : "*",
#         "Condition" : {
#           "StringEquals" : {
#             "aws:PrincipalTag/DatabricksAccountId" : [var.databricks_account_id]
#           }
#         }
#       }
#     ]
#     }
#   )

#   tags = {
#     Project = var.prefix
#     Name    = "${var.prefix}-managed-storage-key"
#   }
# }

# resource "aws_kms_alias" "managed_storage_key_alias" {
#   name          = "alias/${var.prefix}-managed-storage-key"
#   target_key_id = aws_kms_key.managed_storage.key_id
# }