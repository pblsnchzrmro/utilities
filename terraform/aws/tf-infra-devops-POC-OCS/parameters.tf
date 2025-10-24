resource "aws_ssm_parameter" "GitHubConnectionArn" {
  name        = "GitHubConnectionArn"
  description = "Connection to Github Mediaset"
  type        = "String"
  value       = aws_codestarconnections_connection.SNOWFLAKE_DEVOPS-GITHUB.id
}
resource "aws_ssm_parameter" "CrossAccountKmsKeyARN" {
  name        = "CrossAccountKmsKeyARN"
  description = "Clave kms para integracion cross account"
  type        = "String"
  data_type   = "text"
  value       = aws_kms_key.key_mlops.arn
}