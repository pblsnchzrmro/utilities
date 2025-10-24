output "id" {
  description = "Bucket id"
  value       = aws_s3_bucket.bucket.id
}

output "arn" {
  description = "Bucket ARN"
  value       = aws_s3_bucket.bucket.arn
}