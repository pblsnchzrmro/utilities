locals {
  naming_base           = "${var.project_name}-${var.env}-${var.region}"
  naming_base_no_dash   = "${var.project_name}${var.env}${var.region}"
  naming_base_no_region = "${var.project_name}-${var.env}"

  common_tags = {
    Project = var.project_name
  }
}