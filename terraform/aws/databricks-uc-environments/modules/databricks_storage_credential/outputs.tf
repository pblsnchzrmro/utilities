output "role_name" {
  value = aws_iam_role.this.name
}

output "role_arn" {
  value = aws_iam_role.this.arn
}

output "credential_name" {
  value = databricks_credential.credential.name
}

output "credential_id" {
  value = databricks_credential.credential.id
}
