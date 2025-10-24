resource "azurerm_storage_account" "storageaccount" {
  name = "${var.project}${var.environment}storageaccount"

  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location

  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
}

resource "azurerm_role_assignment" "storage_roles" {
  scope                = azurerm_storage_account.storageaccount.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.sit-prueba-service-principal.object_id