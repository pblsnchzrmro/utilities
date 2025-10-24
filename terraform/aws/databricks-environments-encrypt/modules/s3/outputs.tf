output "id" {
  description = "Bucket ID (same as name in S3)."
  value       = aws_s3_bucket.bucket.id
}

output "arn" {
  description = "Bucket ARN."
  value       = aws_s3_bucket.bucket.arn
}

output "name" {
  description = "Bucket name."
  value       = aws_s3_bucket.bucket.bucket
}
