output "role_arn" {
  description = "ARN of the IAM role created for the storage credential"
  value       = aws_iam_role.iam_role.arn
}

output "credential_id" {
  description = "ID of the Unity Catalog storage credential"
  value       = databricks_credential.credential.id
}

output "credential_name" {
  description = "Name of the Unity Catalog storage credential"
  value       = databricks_credential.credential.name
}