data "aws_vpc" "selected" {
  id = var.vpc_id
}

resource "aws_route_table" "this" {
  vpc_id = var.vpc_id

  tags = merge(
    var.tags,
    {
      Name = var.name
    }
  )
}

resource "aws_route" "nat_gateway" {
  count = var.nat_gateway_id != null ? 1 : 0

  route_table_id         = aws_route_table.this.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = var.nat_gateway_id
}

resource "aws_route" "internet_gateway" {
  count = var.internet_gateway_id != null ? 1 : 0

  route_table_id         = aws_route_table.this.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = var.internet_gateway_id
}

resource "aws_route" "vpc_endpoints" {
  for_each = var.vpc_endpoint_routes

  route_table_id             = aws_route_table.this.id
  destination_prefix_list_id = each.key
  vpc_endpoint_id            = each.value
}
