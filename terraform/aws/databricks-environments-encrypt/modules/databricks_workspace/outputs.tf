output "workspace_id" {
  description = "The ID of the created Databricks workspace."
  value       = databricks_mws_workspaces.workspace.workspace_id
}

output "workspace_url" {
  description = "The URL of the created Databricks workspace."
  value       = databricks_mws_workspaces.workspace.workspace_url
}

output "workspace_name" {
  description = "The name of the Databricks workspace."
  value       = databricks_mws_workspaces.workspace.workspace_name
}

output "credentials_id" {
  description = "The ID of the credentials associated with the workspace."
  value       = databricks_mws_credentials.workspace_credentials.credentials_id
}

output "storage_configuration_id" {
  description = "The ID of the storage configuration associated with the workspace."
  value       = databricks_mws_storage_configurations.workspace_storage.storage_configuration_id
}
