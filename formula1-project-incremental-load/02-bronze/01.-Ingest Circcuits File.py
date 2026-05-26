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

# v_batch_id


# COMMAND ----------

# MAGIC %run ../00-common/01.enviroment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/circuits.csv"
table_name = f"{catalog_name}.{bronze_schema}.circuits"


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType([
    StructField('circuitID' , StringType())
    ,StructField('url' , StringType())
    ,StructField('circuitName' , StringType())
    ,StructField('lat' , DoubleType())
    ,StructField('long' , DoubleType())
    ,StructField('locality' , StringType())
    ,StructField('country' , StringType())
])

# COMMAND ----------

# read csv file from using dataframe reader api.
circuits_df = (spark.read.format('csv')
    .option('header','true')
#    .option("inferschema", "true")
    .option('mode','FAILFAST')
    .schema(circuits_schema)
    .load(source_file)
)

# COMMAND ----------

circuits_df.show(5)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_final_df = add_ingestion_metadata(circuits_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. write into bronze layer

# COMMAND ----------

# circuits_final_df = circuits_final_df.withColumn("batch_id",F.lit(v_batch_id))

# COMMAND ----------

# (
#     circuits_final_df
#         .write
#         .format('delta')
#         .mode('overwrite')
#         .partitionBy("batch_id")
#         .option("replaceWhere",f"batch_id = '{v_batch_id}'")
#         .saveAsTable(table_name)
# )

# COMMAND ----------

write_to_bronze(
    input_df=circuits_final_df,
    target_table=table_name,
    batch_id=v_batch_id
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1_incr.bronze.circuits;

# COMMAND ----------

display(spark.table('formula1.bronze.circuits'))

# COMMAND ----------

