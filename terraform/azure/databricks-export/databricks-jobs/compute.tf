resource "databricks_cluster" "pablo_snchez_romeros_cluster_0609_111142_wlfwbcml" {
  spark_version = "12.2.x-scala2.12"
  spark_env_vars = {
    PYSPARK_PYTHON = "/databricks/python3/bin/python3"
  }
  spark_conf = {
    "spark.databricks.cluster.profile" = "singleNode"
    "spark.master"                     = "local[*, 4]"
  }
  single_user_name    = "spablo97@gmail.com"
  runtime_engine      = "STANDARD"
  node_type_id        = "Standard_DS3_v2"
  enable_elastic_disk = true
  data_security_mode  = "LEGACY_SINGLE_USER_STANDARD"
  custom_tags = {
    ResourceClass = "SingleNode"
  }
  cluster_name = "Pablo Sánchez Romero's Cluster"
  azure_attributes {
    spot_bid_max_price = -1
    first_on_demand    = 1
    availability       = "ON_DEMAND_AZURE"
  }
  autotermination_minutes = 20
}
