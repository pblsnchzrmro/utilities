resource "aws_s3_bucket" "root_storage_bucket" {
  bucket        = "${local.naming_base}-rootbucket"
  force_destroy = true
  tags = merge(var.tags, {
    Name = "${local.naming_base}-rootbucket"
  })
}

resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.root_storage_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_ownership_controls" "state" {
  bucket = aws_s3_bucket.root_storage_bucket.id
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_acl" "acl" {
  bucket     = aws_s3_bucket.root_storage_bucket.id
  acl        = "private"
  depends_on = [aws_s3_bucket_ownership_controls.state]
}

resource "aws_s3_bucket_server_side_encryption_configuration" "root_storage_bucket" {
  bucket = aws_s3_bucket.root_storage_bucket.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "root_storage_bucket" {
  bucket                  = aws_s3_bucket.root_storage_bucket.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# resource "aws_s3_bucket_policy" "root_bucket_policy" {
#   bucket     = aws_s3_bucket.root_storage_bucket.id
#   policy     = data.databricks_aws_bucket_policy.this.json
#   depends_on = [aws_s3_bucket_public_access_block.root_storage_bucket]
# }

resource "aws_s3_bucket_policy" "root_bucket_policy" {
  bucket = aws_s3_bucket.root_storage_bucket.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid = "GrantDatabricksAccess",
        Effect = "Allow",
        Principal = {
          AWS = "arn:aws:iam::414351767826:root"
        },
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ],
        Resource = [
          "arn:aws:s3:::${aws_s3_bucket.root_storage_bucket.bucket}/*",
          "arn:aws:s3:::${aws_s3_bucket.root_storage_bucket.bucket}"
        ],
        Condition = {
          StringEquals = {
            "aws:PrincipalTag/DatabricksAccountId" = var.databricks_account_id
          }
        }
      }
    ]
  })

  depends_on = [aws_s3_bucket_public_access_block.root_storage_bucket]
}
