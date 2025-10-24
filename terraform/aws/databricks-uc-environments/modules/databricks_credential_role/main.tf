# --- IAM role assume policy for Databricks ---
data "aws_iam_policy_document" "assume_role_policy" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = local.assume_role_principals
    }

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.databricks_external_id]
    }
  }
}

# --- IAM role for Unity Catalog credential ---
resource "aws_iam_role" "iam_role" {
  name               = var.role_name
  assume_role_policy = data.aws_iam_policy_document.assume_role_policy.json
  tags               = var.tags
}

# --- Inline policy with S3/KMS permissions ---
resource "aws_iam_policy" "iam_policy" {
  name   = "${var.role_name}-policy"
  policy = var.custom_policy_json
  tags   = var.tags
}

# --- Attach IAM policy to role ---
resource "aws_iam_role_policy_attachment" "policy_attachment" {
  role       = aws_iam_role.iam_role.name
  policy_arn = aws_iam_policy.iam_policy.arn
}

# --- Unity Catalog storage credential ---
resource "databricks_credential" "credential" {
  name    = var.credential_name
  purpose = var.purpose
  comment = var.comments

  aws_iam_role {
    role_arn = aws_iam_role.iam_role.arn
  }

  depends_on = [
    aws_iam_role_policy_attachment.policy_attachment
  ]
}
