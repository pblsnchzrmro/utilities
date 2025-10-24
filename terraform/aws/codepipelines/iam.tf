# CodeBuild role
resource "aws_iam_role" "iam_codebuild_role" {
  name               = "CodeBuildServiceRole-databricks-build-project"
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
            "${module.databricks-s3-bucket-codepipeline.arn}",
            "${module.databricks-s3-bucket-codepipeline.arn}/*"
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
