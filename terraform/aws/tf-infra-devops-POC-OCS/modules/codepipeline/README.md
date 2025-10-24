# AWS CodePipeline Terraform Module

## Overview

This Terraform module provisions an AWS CodePipeline with multiple stages to automate CI/CD for a repository hosted on GitHub. The pipeline includes source retrieval, validation, Terraform plan, manual approval (for production), and Terraform apply stages.

## Resources Created

- **AWS CodePipeline**: Manages the CI/CD workflow.
- **S3 Bucket (Artifact Store)**: Stores pipeline artifacts.
- **AWS CodeBuild Projects**: Executes validation, Terraform plan, and apply actions.
- **CodeStar Source Connection**: Triggers pipeline execution on GitHub repository changes.
- **Manual Approval**: Requires approval before applying changes in the `main` branch.

## Module Source

```hcl
module "codepipeline" {
  source = "./modules/codepipeline"

  name                        = "example-pipeline"
  role_arn                    = "arn:aws:iam::123456789012:role/CodePipelineRole"
  s3_location                 = "my-artifact-bucket"
  branch_name                 = "develop"
  github_repository_id        = "my-org/my-repo"
  region                      = "eu-west-1"
  validate_build_project_name = "validate-project"
  plan_build_project_name     = "plan-project"
  apply_build_project_name    = "apply-project"
  source_connection_arn       = "arn:aws:codestar-connections:eu-west-1:123456789012:connection/xyz"
  approval_notification_arn   = "arn:aws:sns:eu-west-1:123456789012:MyApprovalTopic"

  tags = {
    Environment = "dev"
    Project     = "Terraform CodePipeline"
  }
}
```

## Input Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | Yes | Name of the CodePipeline. |
| `role_arn` | string | Yes | IAM role ARN for CodePipeline execution. |
| `s3_location` | string | Yes | S3 bucket for storing pipeline artifacts. |
| `branch_name` | string | Yes | GitHub branch to trigger the pipeline. |
| `github_repository_id` | string | Yes | GitHub repository in `org/repo` format. |
| `region` | string | No | AWS region for the pipeline (default: `eu-west-1`). |
| `validate_build_project_name` | string | Yes | Name of the CodeBuild project for validation. |
| `plan_build_project_name` | string | Yes | Name of the CodeBuild project for Terraform plan. |
| `apply_build_project_name` | string | Yes | Name of the CodeBuild project for Terraform apply. |
| `source_connection_arn` | string | Yes | ARN of the AWS CodeStar source connection. |
| `approval_notification_arn` | string | No | SNS topic ARN for manual approval notifications. |
| `tags` | map(any) | No | Tags to apply to the pipeline resources. |

## Outputs

| Name | Type | Description |
|------|------|-------------|
| `id` | string | The ID of the CodePipeline. |
| `name` | string | The name of the CodePipeline. |
| `arn` | string | The ARN of the CodePipeline. |

## Stages Overview

1. **Source**: Retrieves the latest code from GitHub.
2. **Validation**: Runs CodeBuild to validate the Terraform configuration.
3. **Terraform Plan**: Runs CodeBuild to generate a Terraform execution plan.
4. **Manual Approval** *(Only for production)*: Requires manual review before applying changes.
5. **Terraform Apply**: Runs CodeBuild to apply Terraform changes.

## Example Usage

### Example: Pipeline for a `develop` branch

```hcl
module "codepipeline_dev" {
  source = "./modules/codepipeline"

  name                        = "dev-pipeline"
  role_arn                    = "arn:aws:iam::123456789012:role/CodePipelineRole"
  s3_location                 = "dev-artifact-bucket"
  branch_name                 = "develop"
  github_repository_id        = "my-org/my-repo"
  region                      = "eu-west-1"
  validate_build_project_name = "dev-validate-project"
  plan_build_project_name     = "dev-plan-project"
  apply_build_project_name    = "dev-apply-project"
  source_connection_arn       = "arn:aws:codestar-connections:eu-west-1:123456789012:connection/xyz"
  approval_notification_arn   = "arn:aws:sns:eu-west-1:123456789012:DevApprovalTopic"

  tags = {
    Environment = "dev"
    Project     = "Terraform CodePipeline"
  }
}
```

### Example: Pipeline for a `main` branch with manual approval

```hcl
module "codepipeline_prod" {
  source = "./modules/codepipeline"

  name                        = "prod-pipeline"
  role_arn                    = "arn:aws:iam::123456789012:role/CodePipelineRole"
  s3_location                 = "prod-artifact-bucket"
  branch_name                 = "main"
  github_repository_id        = "my-org/my-repo"
  region                      = "eu-west-1"
  validate_build_project_name = "prod-validate-project"
  plan_build_project_name     = "prod-plan-project"
  apply_build_project_name    = "prod-apply-project"
  source_connection_arn       = "arn:aws:codestar-connections:eu-west-1:123456789012:connection/xyz"
  approval_notification_arn   = "arn:aws:sns:eu-west-1:123456789012:ProdApprovalTopic"

  tags = {
    Environment = "prod"
    Project     = "Terraform CodePipeline"
  }
}
```

## Notes

- The `approval_notification_arn` is required only for the production pipeline, where manual approval is necessary before applying Terraform changes.
- Make sure the IAM role associated with the pipeline has appropriate permissions to access S3, CodeBuild, and CodeStar.
- The pipeline is triggered automatically on new commits to the specified branch.

## Changelog

- **v1.0.0**: Initial release with pipeline setup.
