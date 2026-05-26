# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Driver file
# MAGIC 1. read file from spark api
# MAGIC 2. add metadata column
# MAGIC    - source file
# MAGIC    - ingesting timestamp
# MAGIC 3. write to bronze delta table
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.enviroment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/drivers.json"
table_name = f"{catalog_name}.{bronze_schema}.drivers"


# COMMAND ----------

source_file

# COMMAND ----------

# Define the schema
from pyspark.sql.types import StructType, StructField, StringType, DateType

name_schema = StructType([
    StructField('givenName', StringType()),
    StructField('familyName', StringType())
])

drivers_schema = StructType([
    StructField('driverId', StringType()),
    StructField('name', name_schema),
    StructField('dateOfBirth', DateType()),
    StructField('nationality', StringType()),
    StructField('url', StringType())
])

# COMMAND ----------

# read data from json file
drivers_df = (
        spark.read
        .format('json')
        .schema(drivers_schema)
        .option("mode",'FailFast')
        .load(source_file)
)

# COMMAND ----------

drivers_final_df = add_ingestion_metadata(drivers_df)

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. write into bronze layer

# COMMAND ----------

# (
#     drivers_final_df
#         .write
#         .format('delta')
#         .mode('overwrite')
#         .saveAsTable(table_name)
# )

# COMMAND ----------

write_to_bronze(
    input_df=drivers_final_df,
    target_table=table_name,
    batch_id=v_batch_id
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1_incr.bronze.drivers;

# COMMAND ----------

