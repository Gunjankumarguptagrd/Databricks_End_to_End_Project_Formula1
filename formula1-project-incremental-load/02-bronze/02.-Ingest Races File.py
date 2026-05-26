# Databricks notebook source
# MAGIC %md
# MAGIC # ingesting circuite file
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

source_file = f"{landing_folder_path}/{v_batch_id}/races.csv"
table_name = f"{catalog_name}.{bronze_schema}.races"


# COMMAND ----------

source_file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

races_schema = StructType([
    StructField('season' , IntegerType())
    ,StructField('round' , IntegerType())
    ,StructField('url' , StringType())
    ,StructField('RaceName' , StringType())
    ,StructField('date' , DateType())
    ,StructField('circuitId' , StringType())
    
])

# COMMAND ----------

# read csv file from using dataframe reader api.
races_df = (spark.read.format('csv')
    .option('header','true')
#   .option("inferschema", "true")
    .option('mode','FAILFAST')
    .schema(races_schema)
    .load(source_file)
)

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_final_df = add_ingestion_metadata(races_df)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. write into bronze layer

# COMMAND ----------

# (
#     races_final_df
#         .write
#         .format('delta')
#         .mode('overwrite')
#         .saveAsTable('table_name')
# )

# COMMAND ----------

write_to_bronze(
    input_df=races_final_df,
    target_table=table_name,
    batch_id=v_batch_id
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1_incr.bronze.races;

# COMMAND ----------

display(spark.table('formula1.bronze.races'))

# COMMAND ----------

