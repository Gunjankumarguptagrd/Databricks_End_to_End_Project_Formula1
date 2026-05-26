# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Circuits Data
# MAGIC     1.Read bronze circuits table
# MAGIC     2. Keep only the columns required for analytics (Drop url column)
# MAGIC     3. Standardise column names using snake_case
# MAGIC     (circuitId → circuit_id, circuitName → circuit_name)
# MAGIC     4. Rename columns to make them more meaningful
# MAGIC     (lat → latitude, lng → longitude)
# MAGIC     5. Filter out rows where circuit_id is null (business key validation)
# MAGIC     6. Remove duplicate records
# MAGIC     7. Transform values of columns circuit_name and locality to Title Case
# MAGIC     8. Write the transformed data to silver circuits table

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.enviroment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f'{catalog_name}.{bronze_schema}.circuits'
silver_table = f'{catalog_name}.{silver_schema}.circuits'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze circuit Table

# COMMAND ----------

#circuits_df = spark.read.option('versionAsOf', '0').table(bronze_table)


# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

circuits_df = (
    spark.table(bronze_table).filter(F.col('batch_id') == v_batch_id)
)

# COMMAND ----------

#display(circuits_df)

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

Circuits_selected_df = circuits_df.select(
    F.col('circuitID'),
    F.col('circuitName'),
    F.col('lat'),
    F.col('long'),
    F.col('locality'),
    F.col('country'),
    F.col('ingestion_timestam'),
    F.col('source_file'),
    F.col('batch_id')
)

# COMMAND ----------

#display(Circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 & 4 Standardies Column Name
# MAGIC - circuitID -> circuit_ID
# MAGIC - circuitName -> circuit_Name
# MAGIC - lat -> Latitude
# MAGIC - long -> Longitude
# MAGIC

# COMMAND ----------

circuits_renamed_df = (
    Circuits_selected_df
    .withColumnRenamed('circuitID', 'circuit_ID')
    .withColumnRenamed('circuitName', 'circuit_Name')
    .withColumnRenamed('lat', 'latitude')
    .withColumnRenamed('long', 'longitude')

)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filterout row where circuit id is Null (Business Key Validation)

# COMMAND ----------

# circuits_valid_df = circuits_renamed_df.filter(
#     "circuit_ID is not Null"
# )


# COMMAND ----------

circuits_valid_df = circuits_renamed_df.filter(
    F.col('circuit_ID').isNotNull()

)

# COMMAND ----------

display(circuits_valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove Duplicate Records

# COMMAND ----------

#circuits_distinct_df = circuits_valid_df.distinct()

# COMMAND ----------

circuits_distinct_df = circuits_valid_df.dropDuplicates(['circuit_ID'])

# COMMAND ----------

display(circuits_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform value of column circuit_name , locality in title case

# COMMAND ----------

circuits_final_df = (
    circuits_distinct_df
    .withColumn('circuit_name', F.initcap(F.col("circuit_name")))
    .withColumn('locality', F.initcap(F.col("locality")))
)

# COMMAND ----------

#display(circuits_final_df)

# COMMAND ----------

# circuits_final_df = (
#     circuits_final_df
#         .withColumn("created_timestamp", F.current_timestamp())
#         .withColumn("updated_timestamp", F.current_timestamp())
# )

# COMMAND ----------

write_to_silver(
    input_df=circuits_final_df,
    target_table=silver_table,
    merge_condition="t.circuit_ID = s.circuit_ID",
    columns_to_update=[
        'circuit_name', 
        'latitude', 
        'longitude', 
        'locality', 
        'country', 
        'ingestion_timestam', 
        'source_file', 
        'batch_id' 
    ]
)

# COMMAND ----------

# MAGIC %md  #8. Transformed data to Silver layer

# COMMAND ----------

# circuits_final_df = (
#     circuits_final_df
#         .withColumn("created_timestamp", F.current_timestamp())
#         .withColumn("updated_timestamp", F.current_timestamp())
# )

# COMMAND ----------

# DBTITLE 1,Cell 30
# if not spark.catalog.tableExists(silver_table):

#     (
#         circuits_final_df
#             .write
#             .format("delta")
#             .mode("overwrite")
#             .saveAsTable(silver_table)
#     )

# else:

#     from delta.tables import DeltaTable

#     delta_table = DeltaTable.forName(spark, silver_table)

#     (
#         delta_table.alias("t")
#         .merge(
#             circuits_final_df.alias("s"),
#             "t.circuit_id = s.circuit_id"
#         )
#         .whenMatchedUpdate(
#             condition="s.batch_id >= t.batch_id",
#             set={
#                 "circuit_name": "s.circuit_name",
#                 "latitude": "s.latitude",
#                 "longitude": "s.longitude",
#                 "locality": "s.locality",
#                 "country": "s.country",
#                 "ingestion_timestam": "s.ingestion_timestam",
#                 "source_file": "s.source_file",
#                 "batch_id": "s.batch_id",
#                 "updated_timestamp": "s.updated_timestamp"
#             }
#         )
#         .whenNotMatchedInsertAll()
#         .execute()
#     )

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

