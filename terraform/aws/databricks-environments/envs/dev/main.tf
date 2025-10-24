# module "vpc" {
#   source               = "../../modules/vpc"
#   vpc_name             = var.vpc_name
#   vpc_cidr             = var.vpc_cidr
#   azs                  = var.azs
#   public_subnet_cidrs  = var.public_subnet_cidrs
#   private_subnet_cidrs = var.private_subnet_cidrs
#   tags                 = local.common_tags
# }
data "aws_availability_zones" "available" {
  state = "available"
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "5.7.0"

  name = local.naming_base
  cidr = var.cidr_block
  azs  = data.aws_availability_zones.available.names
  tags = local.common_tags

  enable_dns_support   = true
  enable_dns_hostnames = true
  enable_nat_gateway   = true
  single_nat_gateway   = true
  create_igw           = true

  public_subnets = [cidrsubnet(var.cidr_block, 3, 0)]
  private_subnets = [cidrsubnet(var.cidr_block, 3, 1),
  cidrsubnet(var.cidr_block, 3, 2)]

  manage_default_security_group = true
  default_security_group_name   = "${local.naming_base}-sg"

  default_security_group_egress = [{
    cidr_blocks = "0.0.0.0/0"
  }]

  default_security_group_ingress = [{
    description = "Allow all internal TCP and UDP"
    self        = true
  }]
}

module "s3_root_bucket" {
  source                    = "../../modules/s3"
  name                      = "dd-databricks-rootbucket-${local.naming_base}"
  bucket_versioning_enabled = var.root_bucket_versioning_enabled
  bucket_policy_statements  = [local.databricks_root_bucket_policy]
  force_destroy             = false
  common_tags               = local.common_tags
}

# module "subnets" {
#   source                 = "../../modules/subnets"
#   vpc_id                 = var.vpc_id
#   subnet_cidrs           = var.private_subnet_cidrs
#   public_subnet_cidrs    = var.public_subnet_cidrs
#   route_table_id         = var.private_route_table_id
#   public_route_table_id  = var.public_route_table_id
#   common_tags            = local.common_tags
#   private_custom_tags    = local.private_custom_tags
#   public_custom_tags     = local.public_custom_tags
# }

# module "public_route_table" {
#   source               = "../../modules/routes"
#   vpc_id               = var.vpc_id
#   name                 = "public-rt"
#   internet_gateway_id  = var.igw_id
#   vpc_endpoint_routes  = var.vpc_endpoint_routes
#   tags                 = local.common_tags
# }

module "databricks_sg" {
  source      = "../../modules/security_group"
  name        = "databricks-sg"
  description = "Security group for Databricks workspace"
  vpc_id      = module.vpc.vpc_id
  tags        = local.common_tags

  ingress_rules = var.ingress_rules
  egress_rules  = var.egress_rules
}


# module "private_route_table" {
#   source               = "../../modules/route_table"
#   vpc_id               = var.vpc_id
#   name                 = "private-rt"
#   nat_gateway_id       = var.nat_gateway_id

#   vpc_endpoint_routes = {
#     "pl-6da54004" = "vpce-09880db682fda8ca3"
#     "pl-6fa54006" = "vpce-011f105eefa1c6bc6"
#   }

#   tags = local.common_tags


module "databricks_workspace" {
  source = "../../modules/databricks_workspace"



  databricks_account_id     = var.databricks_account_id
  region                    = var.region
  workspace_name            = "dd-workspace-${local.naming_base}"

  iam_role_name             = "iam-role-project-dev-crossaccount"
  iam_policy_name           = "plicy-crossaccount-${local.naming_base}"
  credentials_name          = "creds-${local.naming_base}"
  storage_configuration_name = "storage-confg-${local.naming_base}"
  bucket_name               = module.s3_root_bucket.name

  network_name              = "dd-network-${local.naming_base}"
  vpc_id                    = module.vpc.vpc_id
  subnet_ids                = module.vpc.private_subnets
  security_group_ids        = [ module.databricks_sg.security_group_id ]

  enable_metastore_assignment = true
  metastore_id                = data.databricks_metastore.metastore.metastore_id

  enable_managed_services_key  = false
  enable_workspace_storage_key = false

  tags = local.common_tags

  depends_on = [ module.s3_root_bucket ]
}
