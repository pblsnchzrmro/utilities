# Bucket S3 Codepipeline
module "snowflake-s3-bucket-codepipeline" {
  source = "./modules/s3"
  bucket = "${var.bucket_name}-${terraform.workspace}-codepipeline"
  tags   = local.tags
}

# YML files #
resource "aws_s3_object" "s3_buildspec_validate" {
  bucket = module.snowflake-s3-bucket-codepipeline.id
  key    = "buildspec_files/buildspec_validate.yml"
  source = "buildspec_files/buildspec_validate.yml"
  acl    = "private"
  etag   = filemd5("buildspec_files/buildspec_validate.yml")
}

resource "aws_s3_object" "s3_buildspec_plan" {
  bucket = module.snowflake-s3-bucket-codepipeline.id
  key    = "buildspec_files/buildspec_plan.yml"
  source = "buildspec_files/buildspec_plan.yml"
  acl    = "private"
  etag   = filemd5("buildspec_files/buildspec_plan.yml")
}

resource "aws_s3_object" "s3_buildspec_apply" {
  bucket = module.snowflake-s3-bucket-codepipeline.id
  key    = "buildspec_files/buildspec_apply.yml"
  source = "buildspec_files/buildspec_apply.yml"
  acl    = "private"
  etag   = filemd5("buildspec_files/buildspec_apply.yml")
}
