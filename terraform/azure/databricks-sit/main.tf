module "network" {
  source = "./modules/network"
  project     = var.project
  environment = var.environment
  location    = var.location
}

module "azuread" {
  source = "./modules/azuread"
  project     = var.project
  environment = var.environment
}

module "databricks" {
  source = "./modules/databricks"
  project     = var.project
  environment = var.environment
  location    = var.location
}

module "storage" {
  source = "./modules/storage"
  project     = var.project
  environment = var.environment
  location    = var.location
}

module "sql" {
  source = "./modules/sql"
  project     = var.project
  environment = var.environment
  location    = var.location
  user_server = var.user_server
  password_server = var.password_server
}
