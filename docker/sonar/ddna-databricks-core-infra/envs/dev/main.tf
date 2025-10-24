module "s3_root_bucket" {
  source                    = "../../modules/s3"
  name                      = "ddna-rootbucket-${local.naming_base}"
  bucket_versioning_enabled = var.root_bucket_versioning_enabled
  # TODO # bucket_policy_statements  = [local.databricks_root_bucket_policy]
  force_destroy             = false
  sse_algorithm             = var.root_bucket_sse_algorithm
  kms_key_id                = var.root_bucket_kms_key_id
  common_tags               = local.common_tags
}


module "databricks_sg" {
  source      = "../../modules/security_group"
  name        = "ddna-sg-${local.naming_base}"
  description = "Security group for Databricks workspace"
  vpc_id      = var.vpc_id
  tags        = local.common_tags

  ingress_rules = var.ingress_rules
  egress_rules  = var.egress_rules
}


module "private_subnets" {
  source          = "../../modules/subnets"
  vpc_id          = var.vpc_id
  azs             = var.azs
  subnet_cidrs    = var.subnet_cidrs
  route_table_id  = var.route_table_id
  common_tags     = local.common_tags
  custom_tags     = local.subnet_custom_tags
}