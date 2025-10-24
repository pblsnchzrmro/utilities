##########################
### CodeBuild Projects ###
##########################

module "github-codebuild-project-validate" {
  source                = "./modules/codebuild"
  name                  = "github-codebuild-project-validate"
  codebuild_role_arn    = aws_iam_role.iam_codebuild_role.arn
  s3_encryption_key_arn = "arn:aws:kms:${var.region}:${data.aws_caller_identity.current.account_id}:alias/aws/s3"

  # Specify the buildspec file path
  buildspec_name = "arn:aws:s3:::snowflake-cicd-pipeline-devops-codepipeline/buildspec_files/buildspec_validate.yml"

  # Environment configuration
  environment_type            = "LINUX_CONTAINER"
  image_name                  = "aws/codebuild/amazonlinux-x86_64-standard:5.0"
  compute_type                = "BUILD_GENERAL1_SMALL"
  image_pull_credentials_type = "CODEBUILD"

  # Log storage location
  logs_path = "${module.snowflake-s3-bucket-codepipeline.id}/codeBuildLogs/validate"

  # Tags for the resource
  tags = local.tags

  depends_on = [aws_iam_role.iam_codebuild_role, module.snowflake-s3-bucket-codepipeline]
}

module "github-codebuild-project-plan" {
  source                = "./modules/codebuild"
  name                  = "github-codebuild-project-plan"
  codebuild_role_arn    = aws_iam_role.iam_codebuild_role.arn
  s3_encryption_key_arn = "arn:aws:kms:${var.region}:${data.aws_caller_identity.current.account_id}:alias/aws/s3"

  # Specify the buildspec file path
  buildspec_name = "arn:aws:s3:::snowflake-cicd-pipeline-devops-codepipeline/buildspec_files/buildspec_plan.yml"

  # Environment configuration
  environment_type            = "LINUX_CONTAINER"
  image_name                  = "aws/codebuild/amazonlinux-x86_64-standard:5.0"
  compute_type                = "BUILD_GENERAL1_SMALL"
  image_pull_credentials_type = "CODEBUILD"

  # Log storage location
  logs_path = "${module.snowflake-s3-bucket-codepipeline.id}/codeBuildLogs/plan"

  # Tags for the resource
  tags = local.tags

  depends_on = [aws_iam_role.iam_codebuild_role, module.snowflake-s3-bucket-codepipeline]
}

module "github-codebuild-project-apply" {
  source                = "./modules/codebuild"
  name                  = "github-codebuild-project-apply"
  codebuild_role_arn    = aws_iam_role.iam_codebuild_role.arn
  s3_encryption_key_arn = "arn:aws:kms:${var.region}:${data.aws_caller_identity.current.account_id}:alias/aws/s3"

  # Specify the buildspec file path
  buildspec_name = "arn:aws:s3:::snowflake-cicd-pipeline-devops-codepipeline/buildspec_files/buildspec_apply.yml"

  # Environment configuration
  environment_type            = "LINUX_CONTAINER"
  image_name                  = "aws/codebuild/amazonlinux-x86_64-standard:5.0"
  compute_type                = "BUILD_GENERAL1_SMALL"
  image_pull_credentials_type = "CODEBUILD"

  # Log storage location
  logs_path = "${module.snowflake-s3-bucket-codepipeline.id}/codeBuildLogs/apply"

  # Tags for the resource
  tags = local.tags

  depends_on = [aws_iam_role.iam_codebuild_role, module.snowflake-s3-bucket-codepipeline]
}
