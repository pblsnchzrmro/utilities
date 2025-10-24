# AWS CodeBuild Terraform Module

## Overview
This Terraform module provisions an AWS CodeBuild project that integrates with AWS CodePipeline to build and test applications. It defines the necessary configurations, including environment settings, artifacts, logging, and IAM roles.

## Features
- Creates an AWS CodeBuild project.
- Configurable build environment (image, compute type, credentials, etc.).
- Integrates with AWS CodePipeline.
- Supports logging to CloudWatch and S3.
- Uses an encryption key for security.
- Allows customization through Terraform variables.

## Usage
Below is an example usage of this module:

```hcl
module "codebuild_project" {
  source                    = "./modules/codebuild"
  name                      = "my-codebuild-project"
  codebuild_role_arn        = aws_iam_role.codebuild.arn
  s3_encryption_key_arn     = aws_kms_key.codebuild.arn
  image_name                = "aws/codebuild/standard:5.0"
  environment_type          = "LINUX_CONTAINER"
  compute_type              = "BUILD_GENERAL1_MEDIUM"
  image_pull_credentials_type = "CODEBUILD"
  build_timeout             = 60
  queued_timeout            = 480
  buildspec_name            = "buildspec.yml"
  logs_path                 = "my-logs-bucket/logs/"
  tags                      = { Environment = "dev" }
}
```

## Inputs
| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `name` | `string` | n/a | Name of the CodeBuild project |
| `codebuild_role_arn` | `string` | n/a | IAM Role ARN for CodeBuild execution |
| `s3_encryption_key_arn` | `string` | n/a | ARN of the KMS encryption key for artifacts |
| `image_name` | `string` | n/a | Docker image used for the build environment |
| `environment_type` | `string` | `LINUX_CONTAINER` | Type of environment |
| `compute_type` | `string` | `BUILD_GENERAL1_SMALL` | Compute instance size for build |
| `image_pull_credentials_type` | `string` | n/a | Credentials type for pulling images |
| `build_timeout` | `number` | `60` | Maximum build duration in minutes |
| `queued_timeout` | `number` | `480` | Maximum queue duration in minutes |
| `buildspec_name` | `string` | n/a | Buildspec file name for the build process |
| `logs_path` | `string` | n/a | S3 path for storing logs |
| `tags` | `map(any)` | `{}` | Key-value tags for the CodeBuild project |

## Outputs
| Output | Description |
|--------|-------------|
| `arn` | The ARN of the created CodeBuild project |
| `name` | The name of the CodeBuild project |

## Notes
- This module is designed to be used within an AWS CodePipeline workflow.
- Ensure the IAM role assigned has appropriate permissions for CodeBuild execution and logging.
