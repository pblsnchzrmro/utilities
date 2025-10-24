resource "azuread_application" "app" {
  name = "${var.project}-${var.environment}-app"
}

resource "azuread_service_principal" "sit-prueba-service-principal" {
  application_id               = azuread_application.app.application_id
  app_role_assignment_required = false
}
