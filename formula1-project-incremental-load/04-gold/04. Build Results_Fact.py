# Databricks notebook source
# MAGIC %md
# MAGIC # Build Results Fact
# MAGIC - 

# COMMAND ----------

dbutils.widgets.text("p_batch_id","")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.enviroment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f'{catalog_name}.{gold_schema}.fact_sessin_results'

# COMMAND ----------

target_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read source Table
# MAGIC - silver.results
# MAGIC - silver.sprints

# COMMAND ----------

# results_df = (
#     spark.table(f"{catalog_name}.{silver_schema}.results")
#         .withColumn("session_type", F.lit("RACE"))
#         .drop("race_name","race_date","ingestion_timestam","source_file")
# )

# COMMAND ----------

# Read results table

results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
        .filter(F.col("batch_id") == v_batch_id)
        .withColumn("session_type", F.lit("RACE"))
        .drop(
            "race_name",
            "race_date",
            "ingestion_timestamp",
            "source_file",
            "batch_id",
            "created_timestamp",
            "updated_timestamp"
        )
)

# COMMAND ----------

# Read sprints table

sprints_df = (
    spark.table(f"{catalog_name}.{silver_schema}.sprints")
        .filter(F.col("batch_id") == v_batch_id)
        .withColumn("session_type", F.lit("SPRINT"))
        .drop(
            "race_name",
            "race_date",
            "ingestion_timestamp",
            "source_file",
            "batch_id",
            "created_timestamp",
            "updated_timestamp"
        )
)

# COMMAND ----------

display(results_df.limit(5))

# COMMAND ----------

# sprints_df = (
#     spark.table(f"{catalog_name}.{silver_schema}.results")
#         .withColumn("session_type", F.lit("SPRINT"))
#         .drop("race_name","race_date","ingestion_timestam","source_file")
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Union Results and Sprints

# COMMAND ----------

results_sprints_df = results_df.union(sprints_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding derived Column
# MAGIC - is_win -> indicate driver won the race
# MAGIC - is_podium -> indicate the driver won podium score (1,2,3)
# MAGIC - has_point -> indicate the driver has scored point

# COMMAND ----------

fact_session_results_df =(
    results_sprints_df
        .withColumn("is_win", F.col("final_position") == 1)
        .withColumn("is_podium", F.col("final_position").between(1,3))
        .withColumn("has_point", F.col("points")>0)
)

# COMMAND ----------

display(fact_session_results_df.filter("season == 2025"))


# COMMAND ----------

# DBTITLE 1,Cell 10
# # write Transformed data to the gold.
# (
#     fact_session_results_df
#         .write
#         .format("delta")
#         .mode("overwrite")
# #        .option("overwriteSchema", "true")
#         .saveAsTable(target_table)
# )

# COMMAND ----------

# DBTITLE 1,Cell 20
# Step 4 - Write the transformed data to the gold fact table

write_to_gold(
    input_df=fact_session_results_df,
    target_table=target_table,
    merge_condition="""
        t.season = s.season
        AND t.round = s.round
        AND t.constructor_id = s.constructor_id
        AND t.driver_id = s.driver_id
        AND t.session_type = s.session_type
    """,
    columns_to_update=[
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "final_position",
        "final_position_text",
        "status",
        "is_win",
        "is_podium",
        "has_point"
    ]
)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

