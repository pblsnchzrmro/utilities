# Bucket S3 Codepipeline
module "databricks-s3-bucket-codepipeline" {
  source = "./modules/s3"
  bucket = "${var.bucket_name}-${terraform.workspace}-codepipeline"
  tags   = local.tags
}

# YML files #
resource "aws_s3_object" "s3_buildspec_validate" {
  bucket = module.databricks-s3-bucket-codepipeline.id
  key    = "buildspec_files/buildspec_validate.yml"
  source = "buildspec_files/buildspec_validate.yml"
  acl    = "private"
  etag   = filemd5("buildspec_files/buildspec_validate.yml")
}

resource "aws_s3_object" "s3_buildspec_plan" {
  bucket = module.databricks-s3-bucket-codepipeline.id
  key    = "buildspec_files/buildspec_plan.yml"
  source = "buildspec_files/buildspec_plan.yml"
  acl    = "private"
  etag   = filemd5("buildspec_files/buildspec_plan.yml")
}

resource "aws_s3_object" "s3_buildspec_apply" {
  bucket = module.databricks-s3-bucket-codepipeline.id
  key    = "buildspec_files/buildspec_apply.yml"
  source = "buildspec_files/buildspec_apply.yml"
  acl    = "private"
  etag   = filemd5("buildspec_files/buildspec_apply.yml")
}


module "codepipeline-use-cases-cicd-dev" {
  # for_each = toset(local.repositories)

  source        = "./modules/codepipeline"
  name          = "dev-pblsnchzrmr-cicd"
  role_arn      = "arn:aws:iam::354861872002:role/CodePipelineStarterTemplate-Terraf-CodePipelineRole-CtERVAdyqIhU"
  s3_location   = module.databricks-s3-bucket-codepipeline.id
  pipeline_type = "cicd"

  validate_build_project_name = module.github-codebuild-project-validate.name
  plan_build_project_name     = module.github-codebuild-project-plan.name
  apply_build_project_name    = module.github-codebuild-project-apply.name

  source_connection_arn     = "arn:aws:codeconnections:eu-west-1:354861872002:connection/13dedc8e-b88b-4adc-acee-3a3aa220dffb"
  approval_notification_arn = null //Develop no tiene notificaciones de aprobacion


  branch_name          = "dev"
  environment_name     = "dev"
  github_repository_id = "pblsnchzrmro/terraform_aws_codepipeline"

  tags = local.tags

  depends_on = [
    module.databricks-s3-bucket-codepipeline,
  ]
}


##########################
### CodeBuild Projects ###
##########################

module "github-codebuild-project-validate" {
  source                = "./modules/codebuild"
  name                  = "github-codebuild-project-validate"
  codebuild_role_arn    = aws_iam_role.iam_codebuild_role.arn

  # Specify the buildspec file path
  buildspec_name = "arn:aws:s3:::${module.databricks-s3-bucket-codepipeline.id}/buildspec_files/buildspec_validate.yml"

  # Environment configuration
  environment_type            = "LINUX_CONTAINER"
  image_name                  = "aws/codebuild/amazonlinux-x86_64-standard:5.0"
  compute_type                = "BUILD_GENERAL1_SMALL"
  image_pull_credentials_type = "CODEBUILD"

  # Log storage location
  logs_path = "${module.databricks-s3-bucket-codepipeline.id}/codeBuildLogs/validate"

  # Tags for the resource
  tags = local.tags

  depends_on = [aws_iam_role.iam_codebuild_role, module.databricks-s3-bucket-codepipeline]
}

module "github-codebuild-project-plan" {
  source                = "./modules/codebuild"
  name                  = "github-codebuild-project-plan"
  codebuild_role_arn    = aws_iam_role.iam_codebuild_role.arn

  # Specify the buildspec file path
  buildspec_name = "arn:aws:s3:::${module.databricks-s3-bucket-codepipeline.id}/buildspec_files/buildspec_plan.yml"

  # Environment configuration
  environment_type            = "LINUX_CONTAINER"
  image_name                  = "aws/codebuild/amazonlinux-x86_64-standard:5.0"
  compute_type                = "BUILD_GENERAL1_SMALL"
  image_pull_credentials_type = "CODEBUILD"

  environment_variables = {
    SECRET_NAME = "/${var.environment}/databricks/credentials"
  }

  #TODO use data block for the secret_name (mandatory create it previusly)

  # Log storage location
  logs_path = "${module.databricks-s3-bucket-codepipeline.id}/codeBuildLogs/plan"

  # Tags for the resource
  tags = local.tags

  depends_on = [aws_iam_role.iam_codebuild_role, module.databricks-s3-bucket-codepipeline]
}

module "github-codebuild-project-apply" {
  source                = "./modules/codebuild"
  name                  = "github-codebuild-project-apply"
  codebuild_role_arn    = aws_iam_role.iam_codebuild_role.arn

  # Specify the buildspec file path
  buildspec_name = "arn:aws:s3:::${module.databricks-s3-bucket-codepipeline.id}/buildspec_files/buildspec_apply.yml"

  # Environment configuration
  environment_type            = "LINUX_CONTAINER"
  image_name                  = "aws/codebuild/amazonlinux-x86_64-standard:5.0"
  compute_type                = "BUILD_GENERAL1_SMALL"
  image_pull_credentials_type = "CODEBUILD"

  environment_variables = {
    SECRET_NAME = "/${var.environment}/databricks/credentials"
  }

  # Log storage location
  logs_path = "${module.databricks-s3-bucket-codepipeline.id}/codeBuildLogs/apply"

  # Tags for the resource
  tags = local.tags

  depends_on = [aws_iam_role.iam_codebuild_role, module.databricks-s3-bucket-codepipeline]
}