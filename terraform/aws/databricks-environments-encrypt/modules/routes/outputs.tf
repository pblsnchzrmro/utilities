output "route_table_id" {
  description = "ID of the created route table"
  value       = aws_route_table.this.id
}
