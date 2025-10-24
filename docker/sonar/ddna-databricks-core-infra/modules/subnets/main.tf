locals {
  subnet_config = zipmap(var.azs, var.subnet_cidrs)
}

resource "aws_subnet" "private" {
  for_each = local.subnet_config

  vpc_id            = var.vpc_id
  cidr_block        = each.value
  availability_zone = each.key

  tags = merge(
    var.common_tags,
    var.custom_tags[each.key]
  )
}

resource "aws_route_table_association" "private" {
  for_each = local.subnet_config

  subnet_id      = aws_subnet.private[each.key].id
  route_table_id = var.route_table_id
}