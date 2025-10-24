# main.tf del módulo
resource "aws_iam_role" "role" {
  name               = var.role_name
  assume_role_policy = var.assume_role_policy
  tags               = var.tags
}

resource "aws_iam_policy" "inline" {
  count  = var.inline_policy_json != null ? 1 : 0
  name   = "${var.role_name}-inline"
  policy = var.inline_policy_json
  tags   = var.tags
}

resource "aws_iam_role_policy_attachment" "inline" {
  count      = var.inline_policy_json != null ? 1 : 0
  role       = aws_iam_role.role.name
  policy_arn = aws_iam_policy.inline[0].arn
}

resource "aws_iam_role_policy_attachment" "managed" {
  for_each   = toset(var.managed_policy_arns)
  role       = aws_iam_role.role.name
  policy_arn = each.value
}
