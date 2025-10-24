resource "azurerm_mssql_server" "sql-server" {
  name                         = "${var.project}-${var.environment}-sqlserver"
  resource_group_name          = azurerm_resource_group.rg.name
  location                     = azurerm_resource_group.rg.location
  version                      = "12.0"
  administrator_login          = var.user_server
  administrator_login_password = var.password_server
  minimum_tls_version          = "1.2"
}

resource "azurerm_mssql_database" "sqldatabase" {
  name           = "${var.project}-${var.environment}-sqldatabase"
  server_id      = azurerm_mssql_server.sql-server.id
  max_size_gb                 = 1
  min_capacity                = 0.5
  read_replica_count          = 0
  read_scale                  = false
  sku_name                    = "GP_S_Gen5_1"
  auto_pause_delay_in_minutes = 60
  zone_redundant              = false
}

resource "azurerm_mssql_virtual_network_rule" "sql-server-vnet-rule-private" {
  name      = "sql-vnet-rule-private"
  server_id = azurerm_mssql_server.sql-server.id
  subnet_id = azurerm_subnet.private.id
}

resource "azurerm_mssql_virtual_network_rule" "sql-server-vnet-rule-public" {
  name      = "sql-vnet-rule-public"
  server_id = azurerm_mssql_server.sql-server.id
  subnet_id = azurerm_subnet.public.id
}