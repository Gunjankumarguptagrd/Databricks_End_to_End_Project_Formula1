# Databricks notebook source
# MAGIC %run ../00-common/01.enviroment-config

# COMMAND ----------

# DBTITLE 1,Cell 2
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{control_schema}");

# COMMAND ----------

spark.sql(f"""
            create table if not exists {catalog_name}.{control_schema}.batch_control
                (
                    batch_id string,
                    status string,
                    created_timestamp TIMESTAMP,
                    updated_timestamp TIMESTAMP
                )
    """
)

# COMMAND ----------

# %sql
# create schema if not exists formula1_incr.control;

# create table if not exists formula1_incr.control.batch-control
#  (
#     batch_id string,
#     status string,
#     created_timestamp string,
#     updated_timestamp string
# )

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN formula1_incr.control;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM formula1_incr.control.batch_control;