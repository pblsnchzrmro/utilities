#######################
### PIPELINES CI/CD ###
#######################

# Pipeline CICD DEV - casos de uso
module "codepipeline-use-cases-cicd-dev" {
  # for_each = toset(local.repositories)

  source        = "./modules/codepipeline"
  name          = "dev-OCS-POC-snowflake-cicd"
  role_arn      = aws_iam_role.iam_pipeline_role.arn
  s3_location   = module.snowflake-s3-bucket-codepipeline.id
  pipeline_type = "cicd"

  validate_build_project_name = module.github-codebuild-project-validate.name
  plan_build_project_name     = module.github-codebuild-project-plan.name
  apply_build_project_name    = module.github-codebuild-project-apply.name

  source_connection_arn     = aws_codestarconnections_connection.SNOWFLAKE_DEVOPS-GITHUB.arn
  approval_notification_arn = null //Develop no tiene notificaciones de aprobacion

  notification_topic_arn   = module.snowflake-cicd-pipeline-error-dev.sns_topic_arn # errors

  branch_name          = "develop"
  environment_name     = "DESARROLLO"
  github_repository_id = "A RELLENAR"

  tags = local.tags

  depends_on = [
    aws_iam_role.iam_pipeline_role,
    module.snowflake-s3-bucket-codepipeline,
    aws_codestarconnections_connection.SNOWFLAKE_DEVOPS-GITHUB,
    module.snowflake-cicd-pipeline-error-dev
  ]
}

# # Pipeline CICD PRE - casos de uso
# module "codepipeline-use-cases-cicd-pre" {
#   for_each = toset(local.repositories)

#   source        = "./modules/codepipeline"
#   name          = "pre-${each.key}-snowflake-cicd"
#   role_arn      = aws_iam_role.iam_pipeline_role.arn
#   s3_location   = module.snowflake-s3-bucket-codepipeline.id
#   pipeline_type = "cicd"

#   validate_build_project_name = module.github-codebuild-project-validate.name
#   plan_build_project_name     = module.github-codebuild-project-plan.name
#   apply_build_project_name    = module.github-codebuild-project-apply.name

#   source_connection_arn     = aws_codestarconnections_connection.SNOWFLAKE_DEVOPS-GITHUB.arn
#   approval_notification_arn = null //Release no tiene notificaciones de aprobacion

#   notification_topic_arn   = module.snowflake-cicd-pipeline-error-pre.sns_topic_arn # errors

#   branch_name          = "release"
#   environment_name     = "PRE"
#   github_repository_id = "${var.github_code_repository_prefix}${each.key}"

#   tags = local.tags

#   depends_on = [
#     aws_iam_role.iam_pipeline_role,
#     module.snowflake-s3-bucket-codepipeline,
#     aws_codestarconnections_connection.SNOWFLAKE_DEVOPS-GITHUB,
#     module.snowflake-cicd-pipeline-error-pre
#   ]
# }

# # Pipeline CICD PRO - casos de uso
# module "codepipeline-use-cases-cicd-pro" {
#   for_each = toset(local.repositories)

#   source        = "./modules/codepipeline"
#   name          = "pro-${each.key}-snowflake-cicd"
#   role_arn      = aws_iam_role.iam_pipeline_role.arn
#   s3_location   = module.snowflake-s3-bucket-codepipeline.id
#   pipeline_type = "cicd"

#   validate_build_project_name = module.github-codebuild-project-validate.name
#   plan_build_project_name     = module.github-codebuild-project-plan.name
#   apply_build_project_name    = module.github-codebuild-project-apply.name

#   source_connection_arn     = aws_codestarconnections_connection.SNOWFLAKE_DEVOPS-GITHUB.arn
#   approval_notification_arn = module.snowflake-cicd-pipeline-approvers-pro.sns_topic_arn

#   notification_topic_arn   = module.snowflake-cicd-pipeline-error-pro.sns_topic_arn # errors

#   branch_name          = "main"
#   environment_name     = "PRO"
#   github_repository_id = "${var.github_code_repository_prefix}${each.key}"

#   tags = local.tags

#   depends_on = [
#     aws_iam_role.iam_pipeline_role,
#     module.snowflake-s3-bucket-codepipeline,
#     aws_codestarconnections_connection.SNOWFLAKE_DEVOPS-GITHUB,
#     module.snowflake-cicd-pipeline-error-pro
#   ]
# }
