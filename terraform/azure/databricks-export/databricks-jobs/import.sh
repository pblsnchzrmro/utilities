#!/bin/sh

set -e

terraform import databricks_cluster.pablo_snchez_romeros_cluster_0609_111142_wlfwbcml "0609-111142-wlfwbcml"
terraform import databricks_cluster.pablo_snchez_romeros_cluster_0612_085744_k5y1rfqk "0612-085744-k5y1rfqk"
terraform import databricks_job.preuba_1062952253808277 "1062952253808277"
terraform import databricks_job.preuba_749062952604182 "749062952604182"
terraform import databricks_job.produccion_468383919147464 "468383919147464"
terraform import databricks_job.produccion_640989304342994 "640989304342994"
terraform import databricks_directory.proyecto_381706814907593 "/proyecto"
terraform import databricks_notebook.proyecto_adeu_381706814907594 "/proyecto/adeu"
terraform import databricks_notebook.users_spablo97_gmail_com_hola_381706814907591 "/Users/spablo97@gmail.com/hola"
