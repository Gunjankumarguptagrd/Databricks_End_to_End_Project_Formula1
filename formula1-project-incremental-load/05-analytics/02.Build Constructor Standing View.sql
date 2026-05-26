-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Build Driver Standing View
-- MAGIC ### Source
-- MAGIC 1. fact_session_resuls
-- MAGIC 2. dim_dim_drivers
-- MAGIC
-- MAGIC ### Output columns
-- MAGIC 1. season
-- MAGIC 2. driver id
-- MAGIC 3. driver name
-- MAGIC 4. natinality
-- MAGIC 5. race starts
-- MAGIC 6. total points
-- MAGIC 7. number of wins
-- MAGIC 8. number of podium
-- MAGIC 9. standing position
-- MAGIC

-- COMMAND ----------

create or replace view formula1.gold.v_constructor_standings
AS
WITH constructor_session_summary AS
(
    SELECT  
        r.season,
        d.driver_id,
        d.driver_name,
        d.nationality,
        COUNT(*) AS race_starts,
        SUM(r.points) AS total_points,
        SUM(CASE WHEN r.is_win = true THEN 1 ELSE 0 END) AS number_of_wins,
        SUM(CASE WHEN r.is_podium = true THEN 1 ELSE 0 END) AS number_of_podiums
    FROM formula1.gold.fact_sessin_results r
    JOIN formula1.gold.dim_drivers d
        ON r.driver_id = d.driver_id
    GROUP BY  
        r.season,
        d.driver_id,
        d.driver_name,
        d.nationality
)

SELECT  
    season,
    driver_id,
    driver_name,
    nationality,
    RANK() OVER (
        PARTITION BY season
        ORDER BY total_points DESC, number_of_wins DESC
    ) AS standing,
    race_starts,
    total_points,
    number_of_wins,
    number_of_podiums
FROM constructor_session_summary;

-- COMMAND ----------

-- SHOW TABLES IN formula1.gold;
select * from formula1.gold.v_constructor_standings where season = 2025

-- COMMAND ----------

