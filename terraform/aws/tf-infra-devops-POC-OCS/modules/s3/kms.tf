# gathering kms key
data "aws_kms_key" "s3-kms-key" {
  key_id = "alias/aws/s3"
}