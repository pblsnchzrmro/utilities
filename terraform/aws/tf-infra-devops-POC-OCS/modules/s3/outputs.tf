# outputs
output "id" {
  value = aws_s3_bucket.s3.id
}
output "arn" {
  value = aws_s3_bucket.s3.arn
}
output "bucket_regional_domain_name" {
  value = aws_s3_bucket.s3.bucket_regional_domain_name
}

# defining output block
output "output_block" {
  value = <<EOF
# S3
|-> id  = ${aws_s3_bucket.s3.id}
|-> arn = ${aws_s3_bucket.s3.arn}
EOF
}
