output "credential_name" {
  description = "Name of the created Databricks credential"
  value       = databricks_credential.credential.name
}

output "credential_id" {
  description = "ID of the created Databricks credential"
  value       = databricks_credential.credential.id
}

output "external_id" {
  value       = databricks_credential.credential.aws_iam_role[0].external_id
  description = "The external_id generated for the AWS IAM role"
}