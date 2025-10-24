from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession


def get_spark(profile_name: str = "DEFAULT") -> SparkSession:
    try:
        from databricks.connect import DatabricksSession
        # Solo usa el perfil si estás fuera de Databricks
        import os
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            # Estás dentro de un entorno Databricks
            return DatabricksSession.builder.getOrCreate()
        return DatabricksSession.builder.profile(profile_name).getOrCreate()
    except Exception:
        return SparkSession.builder.getOrCreate()


def get_dbutils() -> DBUtils:
    spark = get_spark()
    try:
        return DBUtils(spark)
    except Exception as e:
        raise RuntimeError("DBUtils is not available in this environment") from e