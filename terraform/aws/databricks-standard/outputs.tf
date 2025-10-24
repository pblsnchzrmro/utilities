# Outputs para inspeccionar los valores devueltos

output "assume_role_policy" {
  description = "Política JSON para permitir que Databricks asuma el rol"
  value       = data.databricks_aws_assume_role_policy.this.json
}

output "crossaccount_policy" {
  description = "Política de cuenta cruzada para Databricks"
  value       = data.databricks_aws_crossaccount_policy.this.json
}

output "availability_zones" {
  description = "Zonas de disponibilidad en la región actual"
  value       = data.aws_availability_zones.available.names
}

output "bucket_policy" {
  description = "Política de bucket generada para Databricks"
  value       = data.databricks_aws_bucket_policy.this.json
}
