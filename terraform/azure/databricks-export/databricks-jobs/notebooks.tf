resource "databricks_directory" "proyecto_381706814907593" {
  path = "/proyecto"
}
resource "databricks_notebook" "proyecto_adeu_381706814907594" {
  source = "${path.module}/notebooks/proyecto/adeu.py"
  path   = "/proyecto/adeu"
}

resource "databricks_notebook" "proyecto_adeu_381706814907595" {
  source = "${path.module}/notebooks/proyecto/prueba.py"
  path   = "/proyecto/prueba"
}



data "databricks_directory" "shared_1908865488948668" {
  path = "/Shared"
}
data "databricks_directory" "users_1908865488948667" {
  path = "/Users"
}
data "databricks_directory" "users_spablo97_gmail_com_1908865488948669" {
  path = "/Users/spablo97@gmail.com"
}
data "databricks_directory" "users_spablo97_gmail_com_ext_spablo97gmail_onmicrosoft_com_1908865488948671" {
  path = "/Users/spablo97_gmail.com#ext#@spablo97gmail.onmicrosoft.com"
}
resource "databricks_notebook" "users_spablo97_gmail_com_hola_381706814907591" {
  source = "${path.module}/notebooks/Users/spablo97@gmail.com/hola.py"
  path   = "/Users/spablo97@gmail.com/hola"
}


