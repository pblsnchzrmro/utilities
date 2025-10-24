resource "aws_s3_bucket" "uc_storage_bucket" {
  bucket        = "${local.naming_base}-ucbucket-pblscnhz"
  force_destroy = true
  tags = merge(var.tags, {
    Name = "${local.naming_base}-ucbucket"
  })
}

resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.uc_storage_bucket.id
  versioning_configuration {
    status = "Disabled"
  }
}

resource "aws_s3_bucket_ownership_controls" "state" {
  bucket = aws_s3_bucket.uc_storage_bucket.id
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_acl" "acl" {
  bucket     = aws_s3_bucket.uc_storage_bucket.id
  acl        = "private"
  depends_on = [aws_s3_bucket_ownership_controls.state]
}

resource "aws_s3_bucket_server_side_encryption_configuration" "uc_storage_bucket" {
  bucket = aws_s3_bucket.uc_storage_bucket.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "uc_storage_bucket" {
  bucket                  = aws_s3_bucket.uc_storage_bucket.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# resource "aws_s3_bucket_policy" "uc_bucket_policy" {
#   bucket     = aws_s3_bucket.uc_storage_bucket.id
#   policy     = data.databricks_aws_bucket_policy.this.json
#   depends_on = [aws_s3_bucket_public_access_block.uc_storage_bucket]
# }