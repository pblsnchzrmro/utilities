resource "aws_s3_bucket" "bucket" {
  bucket = var.name
  tags = merge(
    var.common_tags,
    { "Name" = var.name },
  )
  force_destroy =  var.force_destroy
}

resource "aws_s3_bucket_versioning" "bucket" {
    bucket = aws_s3_bucket.bucket.id

    versioning_configuration {
      status = var.bucket_versioning_enabled ? "Enabled" : "Disabled"
    }
    
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bucket" {
  bucket = aws_s3_bucket.bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.sse_algorithm
      kms_master_key_id = var.sse_algorithm == "aws:kms" && var.kms_key_id != "" ? var.kms_key_id : null
    }
  }
}

resource "aws_s3_bucket_public_access_block" "bucket" {
  bucket = aws_s3_bucket.bucket.id

  block_public_acls       = true
  block_public_policy     = var.public_policies_enabled ? false : true
  ignore_public_acls      = true
  restrict_public_buckets = var.public_policies_enabled ? false : true
}

resource "aws_s3_bucket_policy" "bucket" {
  count = length(var.bucket_policy_statements) > 0 ? 1 : 0

  bucket = aws_s3_bucket.bucket.id

  policy = data.aws_iam_policy_document.bucket_policy_statements[0].json
}

data "aws_iam_policy_document" "bucket_policy_statements" {
  count = length(var.bucket_policy_statements) > 0 ? 1 : 0
  source_policy_documents = var.bucket_policy_statements
}