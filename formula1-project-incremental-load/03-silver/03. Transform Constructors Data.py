# Databricks notebook source
# MAGIC %md
# MAGIC # Read bronze constructors table
# MAGIC - Keep only the columns required for analytics
# MAGIC (Drop url column)
# MAGIC - Standardise column names using snake_case
# MAGIC (constructorId → constructor_id)
# MAGIC - Rename columns to make them more meaningful
# MAGIC (name → constructor_name)
# MAGIC - Remove duplicate records
# MAGIC - Transform values of column nationality to Title Case
# MAGIC - Write the transformed data to silver constructors table

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.enviroment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f'{catalog_name}.{bronze_schema}.constructors'
silver_table = f'{catalog_name}.{silver_schema}.constructors'

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze circuit Table

# COMMAND ----------

#circuits_df = spark.read.option('versionAsOf', '0').table(bronze_table)


# COMMAND ----------

constructors_df = spark.table(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop url column

# COMMAND ----------

constructors_dropped_df = constructors_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming the column

# COMMAND ----------

constructors_renam_df = (
    constructors_dropped_df
    .withColumnRenamed('constructorId', 'constructor_id')
    .withColumnRenamed('name', 'constructor_name')

)

# COMMAND ----------

display(constructors_renam_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove Duplicate Records

# COMMAND ----------

constructors_distinct_df = constructors_renam_df.dropDuplicates(['constructor_id'])

# COMMAND ----------

display(constructors_distinct_df) # no duplicates found

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform value of column nationality in title case

# COMMAND ----------

constructors_final_df = (
    constructors_distinct_df
    .withColumn('nationality', F.initcap(F.col("nationality")))
)

# COMMAND ----------

# display(constructors_final_df)

# COMMAND ----------

write_to_silver(
    input_df=constructors_final_df,
    target_table=silver_table,
    merge_condition="t.constructor_id = s.constructor_id",
    columns_to_update=[
        'constructor_id', 
        'constructor_name', 
        'nationality', 
        'ingestion_timestam', 
        'source_file', 
        'batch_id'
    ]
)

# COMMAND ----------

# (
#     constructors_final_df
#         .write
#         .format('delta')
#         .mode('overwrite')
#         .saveAsTable(silver_table)
# )

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

