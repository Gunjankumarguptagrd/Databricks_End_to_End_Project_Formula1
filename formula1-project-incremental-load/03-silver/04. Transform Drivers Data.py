# Databricks notebook source
# MAGIC %md
# MAGIC #Read bronze drivers table
# MAGIC - Keep only the columns required for analytics
# MAGIC (Drop url column)
# MAGIC - Standardise column names using snake_case
# MAGIC (driverId → driver_id, dateOfBirth → date_of_birth)
# MAGIC - Concatenate name.givenName and name.familyName to - create a new column called driver_name and transform the value to Title Case
# MAGIC - Remove duplicate records
# MAGIC - Transform values of column nationality to Title Case
# MAGIC - Write the transformed data to silver drivers table

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.enviroment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f'{catalog_name}.{bronze_schema}.drivers'
silver_table = f'{catalog_name}.{silver_schema}.drivers'

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze driver Table

# COMMAND ----------

#circuits_df = spark.read.option('versionAsOf', '0').table(bronze_table)


# COMMAND ----------

drivers_df = spark.table(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop url column

# COMMAND ----------

drivers_dropped_df = drivers_df.drop(F.col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming the column

# COMMAND ----------

drivers_renamed_df = (
    drivers_dropped_df
    .withColumnRenamed('driverId', 'driver_id')
    .withColumnRenamed('dateOfBirth', 'date_of_birth')

)

# COMMAND ----------

display(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concatinate name.givenName and name.familyName. to create new column divers_name

# COMMAND ----------

drivers_concatenated_df = (
    drivers_renamed_df
    .withColumn('driver_name',
                F.initcap(F.concat_ws(" ", F.col("name.givenName"),F.col("name.familyName"))))
    .drop('name')
)

# COMMAND ----------

display(drivers_concatenated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove Duplicate Records

# COMMAND ----------

drivers_distinct_df = drivers_concatenated_df.dropDuplicates(['driver_id'])

# COMMAND ----------

display(drivers_distinct_df) # no duplicates found

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform value of column nationality in title case

# COMMAND ----------

drivers_final_df = (
    drivers_distinct_df
    .withColumn('nationality', F.initcap(F.col("nationality")))
)

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

write_to_silver(
    input_df=drivers_final_df,
    target_table=silver_table,
    merge_condition="t.driver_id = s.driver_id",
    columns_to_update=[
        'driver_id', 
        'date_of_birth', 
        'nationality', 
        'ingestion_timestam', 
        'source_file', 
        'batch_id',
        'driver_name'
    ]
)

# COMMAND ----------

# (
#     drivers_final_df
#         .write
#         .format('delta')
#         .mode('overwrite')
#         .saveAsTable(silver_table)
# )

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

