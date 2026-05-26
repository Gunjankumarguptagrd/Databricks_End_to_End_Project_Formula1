# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Consturctor file
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

source_file = f"{landing_folder_path}/{v_batch_id}/constructors.json"
table_name = f"{catalog_name}.{bronze_schema}.constructors"


# COMMAND ----------

#source_file

# COMMAND ----------

# define schema
constructors_schema = """constructorId String , 
                         name String, 
                         nationality String, 
                         url String"""


# COMMAND ----------

# read data from json file
constructors_df = (
        spark.read
        .format('json')
        .schema(constructors_schema)
        .option("mode",'FailFast')
        .load(source_file)
)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_final_df = add_ingestion_metadata(constructors_df)

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. write into bronze layer

# COMMAND ----------

# (
#     constructors_final_df
#         .write
#         .format('delta')
#         .mode('overwrite')
#         .saveAsTable(table_name)
# )

# COMMAND ----------

write_to_bronze(
    input_df=constructors_final_df,
    target_table=table_name,
    batch_id=v_batch_id
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1_incr.bronze.constructors;