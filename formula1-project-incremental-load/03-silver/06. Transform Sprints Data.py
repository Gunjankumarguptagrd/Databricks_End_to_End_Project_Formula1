# Databricks notebook source
# MAGIC %md
# MAGIC ##Read bronze sprints table
# MAGIC - Keep only the columns required for analytics
# MAGIC (Drop url column)
# MAGIC - Standardise column names using snake_case
# MAGIC constructorId → constructor_id
# MAGIC driverId → driver_id
# MAGIC raceName → race_name
# MAGIC positionText → finish_position_text
# MAGIC - Rename columns to make them more meaningful
# MAGIC date → race_date
# MAGIC grid → grid_position
# MAGIC laps → completed_laps
# MAGIC number → car_number
# MAGIC position → finish_position
# MAGIC - Filter out rows where season, round, constructor_id, or driver_id is null
# MAGIC (Business key validation)
# MAGIC - Remove duplicate records
# MAGIC - Transform values of column race_name to Title Case
# MAGIC - Write the transformed data to silver sprints table

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.enviroment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f'{catalog_name}.{bronze_schema}.sprints'
silver_table = f'{catalog_name}.{silver_schema}.sprints'

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Result Table
# MAGIC - step 1 to 4 read, transform and check data quality

# COMMAND ----------

# results_df = spark.table(bronze_table)

# COMMAND ----------

sprints_df = (
    spark.table(bronze_table)
        .select("season",
                    "round",
                    "constructorId",
                    "driverId",
                    "date",
                    "raceName",
                    "grid",
                    "laps",
                    "number",
                    "points",
                    "position",
                    "positionText",
                    "status",
                    "ingestion_timestam",
                    "source_file",
                    "batch_id")
     .withColumnsRenamed({
                "constructorId": "constructor_id",
                "driverId": "driver_id",
                "raceName": "race_name",
                "date": "race_date",
                "grid": "grid_position",
                "laps": "completed_laps",
                "number": "car_number",
                "position": "final_position",
                "positionText": "final_position_text"
        })

)

# COMMAND ----------

sprints_valid_df = (
    sprints_df
            .filter(
                F.col("season").isNotNull() &
                F.col("round").isNotNull() &
                F.col("constructor_id").isNotNull() &
                F.col("driver_id").isNotNull()
            )
            .dropDuplicates(["season", "round", "constructor_id", "driver_id"])
            
)

# COMMAND ----------

display(sprints_df.count() - sprints_valid_df.count())

# COMMAND ----------

sprints_final_df = (
    sprints_valid_df
        .withColumn("race_name", F.initcap(F.col("race_name")))
)

# COMMAND ----------

display(sprints_final_df)

# COMMAND ----------

# (
#     sprints_final_df
#         .write
#         .format('delta')
#         .mode('overwrite')
#         .saveAsTable(silver_table)
# )

# COMMAND ----------

write_to_silver(
    input_df=sprints_final_df,
    target_table=silver_table,
    merge_condition=" t.season = s.season AND t.round = s.round AND t.constructor_id = s.constructor_id AND t.driver_id = s.driver_id ",
    columns_to_update = [
        "race_name",
        "race_date",
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "final_position",
        "final_position_text",
        "status",
        "ingestion_timestam",
        "source_file",
        "batch_id"
    ]
)



# COMMAND ----------

display(spark.table(silver_table))