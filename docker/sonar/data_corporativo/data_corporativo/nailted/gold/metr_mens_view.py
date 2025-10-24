# Databricks notebook source
df = spark.read.table("people.silver_nailted.metricas")

# COMMAND ----------


df_b = spark.read.table("people.silver_nailted.mensajes")

# Pivot Table B to a wide format
pivoted_df_b = df_b.groupBy(['group','year','month']).pivot("metric").agg(
    expr("first(positive) as positive"),
    expr("first(negative) as negative")
)


# Replace hyphens with underscores in column names
for col_name in pivoted_df_b.columns:
    new_col_name = col_name.replace('-', '_')
    pivoted_df_b = pivoted_df_b.withColumnRenamed(col_name, new_col_name)

pivoted_df_b.display()

# COMMAND ----------



# Join the pivoted Table B with Table A
df_joined = df.join(pivoted_df_b, on=['group','year','month'], how='left')


display(df_joined)

# COMMAND ----------

# Get a list of metrics (excluding 'group')
metrics = [col for col in pivoted_df_b.columns if col not in ['group', 'year', 'month', 'FECHA_CARGA']]

# Dynamically compute the additional columns for each metric
for metric in metrics:
    if 'positive' in metric:
        base_metric = metric.replace("_positive", "")
        df_joined = df_joined.withColumn(f"{base_metric}_total", expr(f"{metric} + {base_metric}_negative"))



df = df_joined