data "aws_iam_policy_document" "passrole_for_uc" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      identifiers = ["arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"]
      type        = "AWS"
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.databricks_account_id]
    }
  }
  statement {
    sid     = "ExplicitSelfRoleAssumption"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      identifiers = ["arn:aws:iam::${var.aws_account_id}:root"]
      type        = "AWS"
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:PrincipalArn"
      values   = ["arn:aws:iam::${var.aws_account_id}:role/${local.naming_base}-uc-access"]
    }
  }
}

resource "aws_iam_policy" "unity_catalog_uc_storage_policy001" {
  name   = "${local.naming_base}-uc-storage-policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Id      = "${local.naming_base}-databricks-unity"
    Statement = [
      {
        "Action": [
				"s3:GetObject",
				"s3:PutObject",
				"s3:DeleteObject",
				"s3:ListBucket",
				"s3:GetBucketLocation",
				"s3:ListBucketMultipartUploads",
				"s3:ListMultipartUploadParts",
				"s3:AbortMultipartUpload"
			]
        "Resource" : [
          aws_s3_bucket.uc_storage_bucket.arn,
          "${aws_s3_bucket.uc_storage_bucket.arn}/*"
        ],
        "Effect" : "Allow"
      },
      {
        "Action" : [
          "sts:AssumeRole"
        ],
        "Resource" : [
          "arn:aws:iam::${var.aws_account_id}:role/${local.naming_base}-uc-access"
        ],
        "Effect" : "Allow"
      }
    ]
  })
  tags = merge(var.tags, {
    Name = "${local.naming_base}-unity-catalog IAM policy"
  })
}


resource "aws_iam_role" "uc_access_role" {
  name = "${local.naming_base}-uc-access"
  assume_role_policy = data.aws_iam_policy_document.passrole_for_uc.json

  tags = merge(var.tags, {
    Name = "${local.naming_base}-uc-access-role"
  })
}

resource "aws_iam_role_policy_attachment" "attach_uc_storage_policy" {
  role       = aws_iam_role.uc_access_role.name
  policy_arn = aws_iam_policy.unity_catalog_uc_storage_policy001.arn
}
