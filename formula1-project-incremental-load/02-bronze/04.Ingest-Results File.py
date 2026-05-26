# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Results file
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

source_file = f"{landing_folder_path}/{v_batch_id}/results"  # folder path
table_name = f"{catalog_name}.{bronze_schema}.results"


# COMMAND ----------

source_file

# COMMAND ----------

# Define the schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

results_schema = StructType([
    StructField("date", DateType()),
    StructField("raceName", StringType()),
    StructField("round", IntegerType()),
    StructField("season", IntegerType()),
    StructField("url", StringType()),
    StructField("constructorId", StringType()),
    StructField("driverId", StringType()),
    StructField("grid", IntegerType()),
    StructField("laps", IntegerType()),
    StructField("number", IntegerType()),
    StructField("points", FloatType()),
    StructField("position", IntegerType()),
    StructField("positionText", StringType()),
    StructField("status", StringType())
])

# COMMAND ----------

# read data from json file
results_df = (
        spark.read
        .format('json')
        .schema(results_schema)
        .option("mode",'FailFast')
        .load(source_file)
)

# COMMAND ----------

results_final_df = add_ingestion_metadata(results_df)

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. write into bronze layer

# COMMAND ----------

# (
#     results_final_df
#         .write
#         .format('delta')
#         .mode('overwrite')
#         .saveAsTable(table_name)
# )

# COMMAND ----------

write_to_bronze(
    input_df=results_final_df,
    target_table=table_name,
    batch_id=v_batch_id
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1_incr.bronze.results;

# COMMAND ----------

# display(spark.table('formula1.bronze.results'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select season, count(*) from formula1.bronze.results 
# MAGIC     group by season 
# MAGIC     order by season desc;