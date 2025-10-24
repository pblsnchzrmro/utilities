resource "databricks_job" "preuba_1062952253808277" {
  webhook_notifications {
  }
  task {
    task_key = "preuba"
    notebook_task {
      source        = "WORKSPACE"
      notebook_path = "/Users/spablo97@gmail.com/prueba"
    }
    existing_cluster_id = databricks_cluster.pablo_snchez_romeros_cluster_0609_111142_wlfwbcml.id
    email_notifications {
    }
  }
  name = "preuba"
  email_notifications {
  }
}
