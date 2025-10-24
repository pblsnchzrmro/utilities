locals {
  naming_base         = "${var.region_short}-${var.project}-${var.env_short}"
  naming_base_no_dash = "${var.region_short}${var.project}${var.env_short}"
  naming_base_no_region = "${var.project}-${var.env_short}"

  # remote_state = {
  #   resource_group_name  = "rg-${local.naming_base}-02"
  #   storage_account_name = "st${local.naming_base_no_dash}02"
  #   container_name       = "tfstates"
  #   key                  = "base.tfstate"
  #   use_azuread_auth     = true
  # }

  common_tags = {

    Environment        = var.env_short == "dev" ? "Development" : (var.env_short == "uat" ? "UAT" : (var.env_short == "pro" ? "Production" : "Unknown"))

  }
}