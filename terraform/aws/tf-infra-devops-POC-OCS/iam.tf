resource "aws_iam_role" "DevOpsProductLaunchRole" {
  name        = "DevOpsProductLaunchRole"
  description = "Product launch role"
  inline_policy {
    name   = "DevOpsProductLaunchPolicy"
    policy = <<EOF
{
"Version": "2012-10-17",
"Statement": [
    {
    "Sid": "CloudFormationPermissionsBase",
    "Effect": "Allow",
    "Action": [
        "cloudformation:CreateStack",
        "cloudformation:UpdateStack",
        "cloudformation:DeleteStack",
        "cloudformation:DescribeStacks",
        "cloudformation:DescribeStackEvents"
    ],
    "Resource": "arn:aws:cloudformation:*:*:stack/SC-*"
    },
    {
    "Sid": "CloudFormationPermissionsForValidate",
    "Effect": "Allow",
    "Action": [
        "cloudformation:GetTemplateSummary",
        "cloudformation:ValidateTemplate"
    ],
    "Resource": "*"
    },
    {
    "Sid": "CodeBuildPermissions",
    "Effect": "Allow",
    "Action": [
        "codebuild:CreateProject",
        "codebuild:DeleteProject",
        "codebuild:UpdateProject"
    ],
    "Resource": [
        "arn:aws:codebuild:*:*:project/*"
    ]
    },
    {
    "Sid": "CodeCommitPermissions",
    "Effect": "Allow",
    "Action": [
        "codecommit:TagResource",
        "codecommit:CreateCommit",
        "codecommit:GetRepository",
        "codecommit:ListRepositories",
        "codecommit:CreateRepository",
        "codecommit:DeleteRepository"
    ],
    "Resource": [
        "arn:aws:codecommit:*:*:*"
    ]
    },
    {
    "Sid": "CodePipelinePermissions",
    "Effect": "Allow",
    "Action": [
        "codepipeline:CreatePipeline",
        "codepipeline:DeletePipeline",
        "codepipeline:GetPipeline",
        "codepipeline:GetPipelineState",
        "codepipeline:StartPipelineExecution",
        "codepipeline:TagResource",
        "codepipeline:UpdatePipeline"
    ],
    "Resource": [
        "arn:aws:codepipeline:*:*:*"
    ]
    },
    {
    "Sid": "ECRPermissions",
    "Effect": "Allow",
    "Action": [
        "ecr:CreateRepository",
        "ecr:DeleteRepository",
        "ecr:TagResource"
    ],
    "Resource": [
        "arn:aws:ecr:*:*:repository/*"
    ]
    },
    {
    "Sid": "EventsPermissions",
    "Effect": "Allow",
    "Action": [
        "events:DescribeRule",
        "events:DeleteRule",
        "events:DisableRule",
        "events:EnableRule",
        "events:PutRule",
        "events:PutTargets",
        "events:RemoveTargets"
    ],
    "Resource": [
        "arn:aws:events:*:*:rule/*"
    ]
    },
    {
    "Sid": "LambdaPermissions",
    "Effect": "Allow",
    "Action": [
        "lambda:AddPermission",
        "lambda:CreateFunction",
        "lambda:DeleteFunction",
        "lambda:TagResource",
        "lambda:GetFunction",
        "lambda:InvokeFunction",
        "lambda:RemovePermission",
        "lambda:GetFunctionConfiguration",
        "lambda:DeleteFunctionEventInvokeConfig",
        "lambda:PutFunctionEventInvokeConfig"
    ],
    "Resource": [
        "arn:aws:lambda:*:*:function:*"
    ]
    },
    {
    "Sid": "LogsPermissions",
    "Effect": "Allow",
    "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:DeleteLogGroup",
        "logs:DeleteLogStream",
        "logs:DescribeLogGroups",
        "logs:DescribeLogStreams",
        "logs:PutRetentionPolicy"
    ],
    "Resource": [
        "arn:aws:logs:*:*:log-group::log-stream:*",
        "arn:aws:logs:*:*:log-group:/aws/apigateway/AccessLogs/*"
    ]
    },
    {
    "Sid": "S3Permissions",
    "Effect": "Allow",
    "Action": [
        "s3:CreateBucket",
        "s3:DeleteBucket",
        "s3:DeleteBucketPolicy",
        "s3:GetBucketPolicy",
        "s3:PutBucketAcl",
        "s3:PutBucketNotification",
        "s3:PutBucketPolicy",
        "s3:PutBucketPublicAccessBlock",
        "s3:PutBucketLogging",
        "s3:PutEncryptionConfiguration",
        "s3:PutBucketCORS",
        "s3:PutBucketTagging",
        "s3:PutObjectTagging",
        "s3:GetObject",
        "s3:List*",
        "s3:PutLifecycleConfiguration"
    ],
    "Resource": "arn:aws:s3:::*"
    },
    {
    "Sid": "CodeStarPermissions",
    "Effect": "Allow",
    "Action": "codestar-connections:PassConnection",
    "Resource": "arn:aws:codestar-connections:*:*:connection/*",
    "Condition": {
        "StringEquals": {
        "codestar-connections:PassedToService": "codepipeline.amazonaws.com"
        }
    }
    },
    {
    "Sid": "SSMPermissions",
    "Effect": "Allow",
    "Action": [
        "ssm:GetParameters"
    ],
    "Resource": [
        "arn:aws:ssm:eu-west-1:${data.aws_caller_identity.current.account_id}:parameter/*"
    ]
    },
    {
    "Sid": "KMSPermissions",
    "Effect": "Allow",
    "Action": [
        "kms:DescribeKey",
        "kms:GenerateDataKey*",
        "kms:Encrypt",
        "kms:ReEncrypt*",
        "kms:Decrypt"
    ],
    "Resource": [
        "arn:aws:kms:eu-west-1:${data.aws_caller_identity.current.account_id}:key/*"
    ]
    },
    {
    "Sid": "IAMPassRolePermissions",
    "Action": [
        "iam:PassRole"
    ],
    "Effect": "Allow",
    "Resource": [
        "arn:aws:iam::*:role/DevOpsProductUseRole",
        "arn:aws:iam::*:role/GigyaWebhookDeploymentRole"
    ]
    },
    {
    "Sid": "SNSPermissions",
    "Action": [
        "sns:*"
    ],
    "Effect": "Allow",
    "Resource": [
        "arn:aws:sns:eu-west-1:*:automatic-approval"
    ]
    }
]
}
EOF
  }
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = [
            "servicecatalog.amazonaws.com"
          ]
        }
      },
    ]
  })
}

resource "aws_iam_role" "DevOpsProductUseRole" {
  name        = "DevOpsProductUseRole"
  description = "Product use role"
  inline_policy {
    name   = "DevOpsProductUsePolicy"
    policy = <<EOF
{
"Version": "2012-10-17",
"Statement": [
    {
    "Sid": "ServiceCatalogPermissions",
    "Action": [
        "servicecatalog:CreatePortfolio",
        "servicecatalog:UpdatePortfolio",
        "servicecatalog:DeletePortfolio",
        "servicecatalog:CreateTagOption",
        "servicecatalog:DeleteTagOption",
        "servicecatalog:UpdateTagOption",
        "servicecatalog:TagResource",
        "servicecatalog:CreateProduct",
        "servicecatalog:DeleteProduct",
        "servicecatalog:UpdateProduct",
        "servicecatalog:DescribeProductAsAdmin",
        "servicecatalog:DescribeProvisioningArtifact",
        "servicecatalog:AssociateTagOptionWithResource",
        "servicecatalog:CreatePortfolioShare",
        "servicecatalog:DisassociateTagOptionFromResource",
        "servicecatalog:DeletePortfolioShare",
        "servicecatalog:AssociateProductWithPortfolio",
        "servicecatalog:CreateConstraint",
        "servicecatalog:DeleteConstraint",
        "servicecatalog:UpdateConstraint",
        "servicecatalog:DisassociateProductFromPortfolio",
        "servicecatalog:DescribeConstraint",
        "servicecatalog:DescribePortfolio",
        "servicecatalog:ListProvisioningArtifacts",
        "servicecatalog:CreateProvisioningArtifact"
    ],
    "Effect": "Allow",
    "Resource": "*"
    },
    {
    "Sid": "CloudFormationPermissions",
    "Action": [
        "cloudformation:CreateChangeSet",
        "cloudformation:CreateStack",
        "cloudformation:DescribeChangeSet",
        "cloudformation:DeleteChangeSet",
        "cloudformation:DeleteStack",
        "cloudformation:DescribeStacks",
        "cloudformation:DescribeStackEvents",
        "cloudformation:DescribeStackResource",
        "cloudformation:ExecuteChangeSet",
        "cloudformation:SetStackPolicy",
        "cloudformation:UpdateStack",
        "cloudformation:ListStacks",
        "cloudformation:ValidateTemplate"
    ],
    "Effect": "Allow",
    "Resource": "arn:aws:cloudformation:eu-west-1:${data.aws_caller_identity.current.account_id}:*"
    },
    {
    "Sid": "CloudWatchPermissions",
    "Action": [
        "cloudwatch:PutMetricData"
    ],
    "Effect": "Allow",
    "Resource": "*"
    },
    {
    "Sid": "CodePipelinePermissions",
    "Effect": "Allow",
    "Action": [
        "codepipeline:CreatePipeline",
        "codepipeline:DeletePipeline",
        "codepipeline:GetPipeline",
        "codepipeline:GetPipelineState",
        "codepipeline:StartPipelineExecution",
        "codepipeline:TagResource",
        "codepipeline:UpdatePipeline",
        "codepipeline:ListPipelineExecutions",
        "codepipeline:PutApprovalResult"
    ],
    "Resource": [
        "arn:aws:codepipeline:*:*:*"
    ]
    },
    {
    "Sid": "CodeBuildPermissions",
    "Action": [
        "codebuild:BatchGetBuilds",
        "codebuild:StartBuild"
    ],
    "Effect": "Allow",
    "Resource": "*"
    },
    {
    "Sid": "CodeCommitPermissions",
    "Action": [
        "codecommit:CancelUploadArchive",
        "codecommit:GetBranch",
        "codecommit:GetCommit",
        "codecommit:GetUploadArchiveStatus",
        "codecommit:UploadArchive"
    ],
    "Effect": "Allow",
    "Resource": "*"
    },
    {
    "Sid": "LogsPermissions",
    "Action": [
        "logs:CreateLogDelivery",
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:DeleteLogDelivery",
        "logs:Describe*",
        "logs:GetLogDelivery",
        "logs:GetLogEvents",
        "logs:ListLogDeliveries",
        "logs:PutLogEvents",
        "logs:PutResourcePolicy",
        "logs:UpdateLogDelivery"
    ],
    "Effect": "Allow",
    "Resource": "*"
    },
    {
    "Sid": "SNSPermissions",
    "Action": [
        "sns:Publish"
    ],
    "Effect": "Allow",
    "Resource": "*"
    },
    {
    "Sid": "S3Permissions",
    "Effect": "Allow",
    "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket",
        "s3:ListAllMyBuckets",
        "s3:GetBucketAcl"
    ],
    "Resource": [
        "arn:aws:s3:::mds-snowflake-devops-artifact/*"
    ]
    },
    {
    "Sid": "IAMPassRolePermissions",
    "Action": [
        "iam:PassRole"
    ],
    "Effect": "Allow",
    "Resource": [
        "arn:aws:iam::*:role/DevOpsProductUseRole",
        "arn:aws:iam::*:role/DevOpsProductLaunchRole"
    ]
    },
    {
    "Sid": "IAMAssumeRolePermissions",
    "Action": [
        "sts:AssumeRole"
    ],
    "Effect": "Allow",
    "Resource": [
        "arn:aws:iam::${data.terraform_remote_state.integracion-snowflake-dev.outputs.account_id}:role/GigyaWebhookDeploymentRole",
        "arn:aws:iam::${data.terraform_remote_state.integracion-snowflake-pre.outputs.account_id}:role/GigyaWebhookDeploymentRole",
        "arn:aws:iam::${data.terraform_remote_state.integracion-snowflake-pro.outputs.account_id}:role/GigyaWebhookDeploymentRole"
    ]
    },
    {
    "Sid": "CodeStarPermissions",
    "Action": [
        "codestar-connections:*"
    ],
    "Effect": "Allow",
    "Resource": "*"
    },
    {
    "Sid": "SSMPermissions",
    "Effect": "Allow",
    "Action": [
        "ssm:GetParameters"
    ],
    "Resource": [
        "arn:aws:ssm:eu-west-1:${data.aws_caller_identity.current.account_id}:parameter/*"
    ]
    },
    {
    "Sid": "KMSCrossAccountPermissions",
    "Action": [
        "kms:DescribeKey",
        "kms:GenerateDataKey*",
        "kms:Encrypt",
        "kms:ReEncrypt*",
        "kms:Decrypt"
    ],
    "Effect": "Allow",
    "Resource": [
        "arn:aws:kms:eu-west-1:${data.aws_caller_identity.current.account_id}:key/${aws_kms_key.key_mlops.id}"
    ]
    }
]
}
EOF
  }
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = "AWSServices"
        Principal = {
          Service = [
            "cloudformation.amazonaws.com",
            "codebuild.amazonaws.com",
            "codepipeline.amazonaws.com"
          ]
        }
      },
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = "AWSAccounts"
        Principal = {
          "AWS" : "arn:aws:iam::${data.terraform_remote_state.automation-devops-infra.outputs.account_id}:root"
        }
      },
    ]
  })
}
