resource "aws_codepipeline" "pipeline" {
  name     = var.name
  role_arn = var.role_arn
  pipeline_type = "V2"

  artifact_store {
    location = var.s3_location
    type     = "S3"
  }

  # Trigger with a push to the corresponding branch
  trigger {
    provider_type = "CodeStarSourceConnection"

    git_configuration {
      source_action_name = "Source"

      push {
        branches {
          includes = [var.branch_name]
        }
      }
    }
  }

  # Source stage
  stage {
    name = "Source"
    action {
      name             = "Source"
      category         = "Source"
      owner            = "AWS"
      provider         = "CodeStarSourceConnection"
      version          = "1"
      run_order        = 1
      input_artifacts  = []
      output_artifacts = ["SourceArtifact"]
      configuration = {
        BranchName           = var.branch_name,
        ConnectionArn        = var.source_connection_arn,
        DetectChanges        = "false",
        FullRepositoryId     = var.github_repository_id
        OutputArtifactFormat = "CODE_ZIP"
      }
      region    = var.region
      namespace = "SourceVariables"
    }
  }

  # Validation stage
  stage {
    name = "Validation"
    action {
      name             = "Validate"
      category         = "Build"
      owner            = "AWS"
      provider         = "CodeBuild"
      version          = "1"
      run_order        = 1
      input_artifacts  = ["SourceArtifact"]
      output_artifacts = ["ValidationArtifact"]
      configuration = {
        ProjectName          = var.validate_build_project_name
      }
      region    = var.region
      namespace = "BuildVariables"
    }
  }


  # Terraform Plan stage
  stage {
    name = "TFPlan"
    action {
      name             = "TFPlan"
      category         = "Build"
      owner            = "AWS"
      provider         = "CodeBuild"
      version          = "1"
      run_order        = 1
      input_artifacts  = ["SourceArtifact"]
      output_artifacts = ["TFPlanArtifacts"]
      configuration = {
        ProjectName          = var.plan_build_project_name
        EnvironmentVariables = jsonencode([
         {
          name  = "BRANCH_NAME"
          value = var.branch_name
          type  = "PLAINTEXT"
        },
        {
          name  = "CODEPIPELINE_NAME"
          value = var.name
          type  = "PLAINTEXT"
        },
        {
          name  = "CODEPIPELINE_ARTIFACTS_BUCKET"
          value = var.s3_location
          type  = "PLAINTEXT"
        }
      ])
      }
      region = var.region
      namespace = "PlanVariables"
    }
  }

  # Manual Approval stage (only for PRO environment)
  # dynamic "stage" {
  #   for_each = var.branch_name == "develop" ? [1] : []
  #   content {
  #     name = "Approval"
  #     action {
  #       name     = "Approval"
  #       category = "Approval"
  #       owner    = "AWS"
  #       provider = "Manual"
  #       version  = "1"
  #       configuration = {
  #         CustomData = "Please review the Terraform plan before applying changes to PRO environment"
  #         # ExternalEntityLink = "#{PlanVariables.CODEBUILD_BUILD_URL}"
  #         # ExternalEntityLink = "https://console.aws.amazon.com/codesuite/codebuild/projects/${var.plan_build_project_name}"
  #         # ExternalEntityLink = "#{PlanVariables.ARTIFACT_URL}"
  #         ExternalEntityLink = "https://${var.region}.console.aws.amazon.com/codesuite/codebuild/projects/${var.plan_build_project_name}/build/#{PlanVariables.CODEBUILD_BUILD_ID}/reports"
  #         NotificationArn = var.approval_notification_arn
  #       }
  #       region = var.region
  #     }
  #   }
  # }

  # Terraform Apply stage
  stage {
    name = "TFApply"
    dynamic "action" {
      for_each = var.branch_name == "main" ? [1] : []
      content{
        name = "ManualApproval"
        category = "Approval"
        owner    = "AWS"
        provider = "Manual"
        version  = "1"
        run_order  = 1
        configuration = {
            CustomData = "Please review the Terraform plan before applying changes"
            NotificationArn = var.approval_notification_arn
          }
        region = var.region
      }
    }
    action {
      name             = "TFApply"
      category         = "Build"
      owner            = "AWS"
      provider         = "CodeBuild"
      version          = "1"
      run_order        = 2
      input_artifacts  = ["SourceArtifact", "TFPlanArtifacts"]
      output_artifacts = ["TFApplyArtifact"]
      configuration = {
        ProjectName          = var.apply_build_project_name
        PrimarySource = "SourceArtifact" 
        EnvironmentVariables = jsonencode([
         {
          name  = "BRANCH_NAME"
          value = var.branch_name
          type  = "PLAINTEXT"
        },
        {
          name  = "HAS_CHANGES"
          value = "#{PlanVariables.HAS_CHANGES}"
          type  = "PLAINTEXT"
        }
      ])
      }
      region    = var.region
    }
  }

  # tags = merge(var.tags, { Name = var.name })
}


# CloudWatch Event Rule
resource "aws_cloudwatch_event_rule" "pipeline_failure" {
  name        = "${var.name}-failure-notification"
  description = "Capture CodePipeline failure events for ${var.name}"

  event_pattern = jsonencode({
    source      = ["aws.codepipeline"]
    detail-type = ["CodePipeline Pipeline Execution State Change"]
    detail = {
      pipeline = [var.name]
      state    = ["FAILED"]
    }
  })
}

# CloudWatch Event Target
# resource "aws_cloudwatch_event_target" "pipeline_failure_target" {
#   rule      = aws_cloudwatch_event_rule.pipeline_failure.name
#   target_id = "SendToSNS"
#   arn       = var.notification_topic_arn

#   input_transformer {
#     input_paths = {
#       state      = "$.detail.state"
#       execution  = "$.detail.execution-id"
#       time       = "$.time"
#       failureReason = "$.detail.execution-result.error-details.message"
#     }
#     input_template = jsonencode(<<EOF
# ❌ Pipeline ${var.name} Failure Alert for env ${var.environment_name}. Check details in AWS Console: https://${var.region}.console.aws.amazon.com/codesuite/codepipeline/pipelines/${var.name}/view
# EOF
#     )
#   #   input_template = jsonencode({
#   #     "Subject":"❌ Pipeline Failure Alert ❌",
#   #     "Pipeline":"${var.name}",
#   #     "Env":"${var.environment_name}",
#   #     "Link to pipeline":"https://${var.region}.console.aws.amazon.com/codesuite/codepipeline/pipelines/${var.name}/view",
#   #     "Plain text URL":"${var.region}.console.aws.amazon.com/codesuite/codepipeline/pipelines/${var.name}/view"
#   #  })
#   }
# }

