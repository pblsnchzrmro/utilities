resource "databricks_metastore" "ireland_metastore" {
  provider = databricks.mws
  name          = "ireland-metastore"
  owner         = "pblsnchzrmr@gmail.com"
  region        = "eu-west-1"
  force_destroy = false
}

locals {
  group_names = [
    "prueba",
  ]
  sp_client_ids = [
    "d62bf633-ffca-4cdf-aad0-d6441418828f"  ]
}

resource "databricks_group" "metastoreowner" {
  provider     = databricks.mws
  display_name = "metastoreowner"
}

data "databricks_group" "subgroups" {
  for_each     = toset(local.group_names)
  provider     = databricks.mws
  display_name = each.key
}

data "databricks_service_principal" "sps" {
  for_each       = toset(local.sp_client_ids)
  provider       = databricks.mws
  application_id = each.key
}

resource "databricks_group_member" "subgroups_in_group" {
  for_each   = data.databricks_group.subgroups
  provider   = databricks.mws
  group_id   = databricks_group.metastoreowner.id
  member_id  = each.value.id
}

resource "databricks_group_member" "sps_in_group" {
  for_each   = data.databricks_service_principal.sps
  provider   = databricks.mws
  group_id   = databricks_group.metastoreowner.id
  member_id  = each.value.id
}