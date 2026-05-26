# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Races Data
# MAGIC - Read bronze races table
# MAGIC Keep only the columns required for analytics (Drop url column)
# MAGIC - Standardize column names using snake_case
# MAGIC     raceName → race_name
# MAGIC     circuitId → circuit_id
# MAGIC - Rename columns to make them more meaningful
# MAGIC     date → race_date
# MAGIC - Remove duplicate records
# MAGIC - Transform values of column race_name to Title Case
# MAGIC - Write the transformed data to silver races table

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.enviroment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f'{catalog_name}.{bronze_schema}.races'
silver_table = f'{catalog_name}.{silver_schema}.races'

# COMMAND ----------

silver_table

# COMMAND ----------

bronze_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze circuit Table

# COMMAND ----------

#circuits_df = spark.read.option('versionAsOf', '0').table(bronze_table)


# COMMAND ----------

races_df = spark.table(bronze_table)

# COMMAND ----------

display(races_df)

# COMMAND ----------

# Circuits_selected_df = circuits_df.select(
#     'circuitID',
#     'circuitName',
#     'lat',
#     'long',
#     'locality',
#     'country',
#     'ingestion_timestam',
#     'source_file'
# )

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

races_selected_df = races_df.select(
    F.col('season'),
    F.col('round'),
    F.col('RaceName'),
    F.col('date'),
    F.col('circuitId'),
    F.col('ingestion_timestam'),
    F.col('source_file'),
    F.col('batch_id')
)

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 & 4 Standardies Column Name
# MAGIC - circuitId -> circuit_ID
# MAGIC - RaceName -> race_Name
# MAGIC - date -> race_date

# COMMAND ----------

races_renamed_df = (
    races_selected_df
    .withColumnRenamed('circuitId', 'circuit_ID')
    .withColumnRenamed('RaceName', 'race_name')
    .withColumnRenamed('date', 'race_date')

)

# COMMAND ----------

# display(races_valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove Duplicate Records

# COMMAND ----------

#circuits_distinct_df = circuits_valid_df.distinct()

# COMMAND ----------

races_distinct_df = races_renamed_df.dropDuplicates(['season','round'])

# COMMAND ----------

display(races_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform value of column race_name in title case

# COMMAND ----------

races_final_df = (
    races_distinct_df
    .withColumn('race_name', F.initcap(F.col("race_name")))
)

# COMMAND ----------

write_to_silver(
    input_df=races_final_df,
    target_table=silver_table,
    merge_condition="t.circuit_ID = s.circuit_ID",
    columns_to_update=[
        'season', 
        'round', 
        'race_name', 
        'race_date', 
        'circuit_ID', 
        'ingestion_timestam', 
        'source_file', 
        'batch_id'
    ]
)

# COMMAND ----------

# (
#     races_final_df
#         .write
#         .format('delta')
#         .mode('overwrite')
#         .saveAsTable(silver_table)
# )

# COMMAND ----------

display(spark.table(silver_table))