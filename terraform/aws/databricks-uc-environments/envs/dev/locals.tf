locals {
  naming_base           = "${var.project_name}-${var.env}-${var.region}"
  naming_base_no_dash   = "${var.project_name}${var.env}${var.region}"
  naming_base_no_region = "${var.project_name}-${var.env}"

  databricks_uc_main_storage_role_name = "dd-${local.naming_base}-uc-stoage-role"


  common_tags = {
    Project = var.project_name
  }
}