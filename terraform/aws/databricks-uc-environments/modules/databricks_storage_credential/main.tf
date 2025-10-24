data "aws_iam_policy_document" "assume_role_policy" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      identifiers = [var.uc_master_role_arn]
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
      values   = ["arn:aws:iam::${var.aws_account_id}:role/${var.role_name}"]
    }
  }
}

resource "aws_iam_role" "this" {
  name               = var.role_name
  assume_role_policy = data.aws_iam_policy_document.assume_role_policy.json
  tags               = var.tags
}

data "aws_iam_policy_document" "s3_policy" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "s3:ListBucketMultipartUploads",
      "s3:ListMultipartUploadParts",
      "s3:AbortMultipartUpload"
    ]
    resources = [
      var.bucket_arn,
      "${var.bucket_arn}/*"
    ]
  }

  statement {
    effect = "Allow"
    actions = ["sts:AssumeRole"]
    resources = [
      "arn:aws:iam::${var.aws_account_id}:role/${var.role_name}"
    ]
  }

  dynamic "statement" {
    for_each = var.enable_kms ? [1] : []
    content {
      effect  = "Allow"
      actions = [
        "kms:Decrypt",
        "kms:Encrypt",
        "kms:GenerateDataKey*"
      ]
      resources = [var.kms_key_arn]
    }
  }
}

resource "aws_iam_policy" "s3_access_policy" {
  name   = "${var.role_name}-s3-policy"
  policy = data.aws_iam_policy_document.s3_policy.json
  tags   = var.tags
}

resource "aws_iam_role_policy_attachment" "attach" {
  role       = aws_iam_role.this.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}

resource "databricks_credential" "credential" {
  name    = var.credential_name
  purpose = "STORAGE"
  comment = "Managed by TF"

  aws_iam_role {
    role_arn = aws_iam_role.this.arn
  }

  depends_on = [
    aws_iam_role_policy_attachment.attach
  ]
  force_destroy = true
  skip_validation = true
}

resource "databricks_grants" "this" {
  count = length(var.storage_credential_permissions) == 0 ? 0 : 1

  credential = databricks_credential.credential.id

  dynamic "grant" {
    for_each = var.storage_credential_permissions
    content {
      principal  = grant.value.principal
      privileges = grant.value.privileges
    }
  }
}
