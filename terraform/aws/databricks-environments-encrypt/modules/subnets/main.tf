locals {
  private_subnet_config = var.subnet_cidrs
  public_subnet_config  = var.public_subnet_cidrs
}

resource "aws_subnet" "private" {
  for_each = local.private_subnet_config

  vpc_id            = var.vpc_id
  cidr_block        = each.value
  availability_zone = each.key

  tags = merge(
    var.common_tags,
    var.private_custom_tags[each.key]
  )
}

resource "aws_route_table_association" "private" {
  for_each = local.private_subnet_config

  subnet_id      = aws_subnet.private[each.key].id
  route_table_id = var.route_table_id
}

resource "aws_subnet" "public" {
  for_each = local.public_subnet_config

  vpc_id                  = var.vpc_id
  cidr_block              = each.value
  availability_zone       = each.key
  map_public_ip_on_launch = true

  tags = merge(
    var.common_tags,
    var.public_custom_tags[each.key]
  )
}

resource "aws_route_table_association" "public" {
  for_each = local.public_subnet_config

  subnet_id      = aws_subnet.public[each.key].id
  route_table_id = var.public_route_table_id
}