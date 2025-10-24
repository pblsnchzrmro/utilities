### CodeBuild Project Resource ####
resource "aws_codebuild_project" "codebuild-project" {
  name           = var.name
  build_timeout  = var.build_timeout
  queued_timeout = var.queued_timeout
  service_role   = var.codebuild_role_arn
  encryption_key = var.s3_encryption_key_arn

  artifacts {
    type = "CODEPIPELINE"
  }

  cache {
    type = "NO_CACHE"
  }

  environment {
    type                        = var.environment_type
    image                       = var.image_name
    compute_type                = var.compute_type
    image_pull_credentials_type = var.image_pull_credentials_type
  }

  logs_config {
    cloudwatch_logs {
      status = "ENABLED"
    }

    s3_logs {
      status              = "ENABLED"
      location            = var.logs_path
      encryption_disabled = false
    }
  }

  source {
    type = "CODEPIPELINE"
    buildspec = var.buildspec_name
  }

  tags = merge(var.tags, { Name = var.name })
}