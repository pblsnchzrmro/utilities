# Pipeline role
resource "aws_iam_role" "iam_pipeline_role" {
  name               = "CodePipelineServiceRole-${var.region}-snowflake-cicd"
  path               = "/service-role/"
  assume_role_policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "codepipeline.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        },
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::985079483454:root"
            },
            "Action": "sts:AssumeRole",
            "Condition": {}
        }
    ]
}
EOF
}

resource "aws_iam_policy" "cicd_pipeline_policy" {
  name        = "cicd_pipeline_policy"
  description = "Politica creada para el role de la pipeline CICD. Permite acceso completo a los recursos necesarios para acceder al backend de terraform."
  path        = "/"
  policy = jsonencode(
    {
      Statement = [
        {
          Action = [
            "s3:*",
            "dynamodb:*",
            "logs:*",
          ]
          Effect   = "Allow"
          Resource = "*"
        },
        {
          Effect = "Allow"
          Action = [
            "codestar-connections:UseConnection",
            "codestar-connections:GetConnection"
          ]
          Resource = aws_codestarconnections_connection.SNOWFLAKE_DEVOPS-GITHUB.arn
        },
        {
          Effect = "Allow"
          Action = [
            "codebuild:StartBuild",
            "codebuild:BatchGetBuilds",
            "codebuild:BatchGetProjects",
            "codebuild:StopBuild"
          ]
          Resource = "arn:aws:codebuild:eu-west-1:851725324220:project/*"
        },
        {
          Effect = "Allow"
          Action = [
            "codepipeline:PutApprovalResult",
            "codepipeline:GetPipelineExecution",
            "codepipeline:ListPipelineExecutions"
          ]
          Resource = "arn:aws:codepipeline:eu-west-1:851725324220:*"
        },
        {
          Effect = "Allow"
          Action = [
            "sns:Publish"
          ]
          Resource = [
            module.snowflake-cicd-pipeline-approvers-pro.sns_topic_arn
          ]
        }
      ]
      Version = "2012-10-17"
    }
  )
}

resource "aws_iam_role_policy_attachment" "iam_pipeline_role_code" {
  role       = aws_iam_role.iam_pipeline_role.name
  policy_arn = aws_iam_policy.cicd_pipeline_policy.arn
}

# CodeBuild role
resource "aws_iam_role" "iam_codebuild_role" {
  name               = "CodeBuildServiceRole-snowflake-build-project"
  path               = "/service-role/"
  assume_role_policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "codebuild.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF
}

resource "aws_iam_policy" "iam_codebuild_service_role_policy" {
  name        = "iam_codebuild_service_role_policy"
  description = "Policy for the CodeBuild service role."
  path        = "/"
  policy = jsonencode(
    {
      Version = "2012-10-17",
      Statement = [
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:PutObject",
            "s3:ListBucket"
          ]
          Resource = [
            "${module.snowflake-s3-bucket-codepipeline.arn}",
            "${module.snowflake-s3-bucket-codepipeline.arn}/*"
          ]
        },
        {
          Action   = "sts:AssumeRole"
          Effect   = "Allow"
          Resource = "arn:aws:iam::985079483454:role/MDSCodepipeline"
        },
        {
          Effect = "Allow"
          Action = [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents"
          ]
          Resource = "arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/codebuild/*"
        },
        {
          Effect = "Allow"
          Action = [
            "codepipeline:GetPipelineState"
          ]
          Resource = "arn:aws:codepipeline:${var.region}:${data.aws_caller_identity.current.account_id}:*"
        }
        # ,{
        #   Effect   = "Allow"
        #   Action   = [
        #     "codebuild:CreateReportGroup",
        #     "codebuild:CreateReport",
        #     "codebuild:UpdateReport",
        #     "codebuild:BatchPutTestCases"
        #   ]
        #   Resource = "arn:aws:codebuild:${var.region}:${data.aws_caller_identity.current.account_id}:report-group/*"
        # }
      ]
    }
  )
}

resource "aws_iam_role_policy_attachment" "iam_codebuild_role_attachment" {
  role       = aws_iam_role.iam_codebuild_role.name
  policy_arn = aws_iam_policy.iam_codebuild_service_role_policy.arn
}
