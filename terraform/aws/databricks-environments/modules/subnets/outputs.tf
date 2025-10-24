output "private_subnet_ids" {
  description = "Map of private subnet IDs by availability zone"
  value       = { for az, subnet in aws_subnet.private : az => subnet.id }
}

output "public_subnet_ids" {
  description = "Map of public subnet IDs by availability zone"
  value       = { for az, subnet in aws_subnet.public : az => subnet.id }
}
