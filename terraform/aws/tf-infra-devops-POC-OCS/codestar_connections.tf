resource "aws_codestarconnections_connection" "SNOWFLAKE_DEVOPS-GITHUB" {
  name          = "GitHubConnection"
  provider_type = "GitHub"
}