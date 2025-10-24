resource "aws_s3_bucket" "s3" {
  bucket = lower(var.bucket)

  # defining lifecycles
  dynamic "lifecycle_rule" {
    for_each = var.lifecycle_rule
    content {
      id      = lifecycle_rule.value.id
      prefix  = lifecycle_rule.value.prefix
      enabled = true

      # defining transition
      dynamic "transition" {
        for_each = lookup(lifecycle_rule.value, "transition", [])
        content {
          days          = transition.value.days
          storage_class = transition.value.storage_class
        }
      }

      # defining expiration days
      dynamic "expiration" {
        for_each = lookup(lifecycle_rule.value, "expiration", null) != null ? [lifecycle_rule.value.expiration] : []
        content {
          days = expiration.value
        }
      }
    }
  }

  tags = merge(var.tags, { Name = var.bucket })
}


# default = false
resource "aws_s3_bucket_versioning" "s3_versioning" {
  bucket = aws_s3_bucket.s3.id
  versioning_configuration {
    status = var.versioning
  }
}

# blocking bucket public access
resource "aws_s3_bucket_public_access_block" "s3-block" {
  bucket = aws_s3_bucket.s3.id

  # defining blocks
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}