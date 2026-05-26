-- Databricks notebook source
-- MAGIC %md
-- MAGIC #01-Setup Project Environment
-- MAGIC 1. create external location gunjanadls2
-- MAGIC 2. create catalog formula1
-- MAGIC 3. create schema landing bronze, silver & gold
-- MAGIC 4. create volume file in the landing schema

-- COMMAND ----------

-- DBTITLE 1,Cell 2
LIST 'abfss://formula1-incr@gunjanadlsgen2.dfs.core.windows.net/landing';

-- COMMAND ----------

-- DBTITLE 1,Cell 3
CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_ext_loc_demo_incr
URL 'abfss://formula1-incr@gunjanadlsgen2.dfs.core.windows.net'
WITH (STORAGE CREDENTIAL `dbt_credential`)
COMMENT 'external location for formula1_incr container';

-- COMMAND ----------

show catalogs

-- COMMAND ----------

-- DBTITLE 1,Cell 5
create catalog if not exists formula1_incr
    managed location 'abfss://formula1-incr@gunjanadlsgen2.dfs.core.windows.net/'
    comment 'this is the main catalog for formula1 project'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - ## Create SCHEMA BRONZE, SILVER AND GOLD schema

-- COMMAND ----------

-- DBTITLE 1,Cell 7
CREATE SCHEMA IF NOT EXISTS formula1_incr.landing;
CREATE SCHEMA IF NOT EXISTS formula1_incr.bronze
   managed location 'abfss://formula1-incr@gunjanadlsgen2.dfs.core.windows.net';
CREATE SCHEMA IF NOT EXISTS formula1_incr.silver
   managed location 'abfss://formula1-incr@gunjanadlsgen2.dfs.core.windows.net';
CREATE SCHEMA IF NOT EXISTS formula1_incr.gold
   managed location 'abfss://formula1-incr@gunjanadlsgen2.dfs.core.windows.net';


-- COMMAND ----------

use catalog formula1_incr;
show schemas

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS formula1_incr.landing.files
LOCATION 'abfss://formula1-incr@gunjanadlsgen2.dfs.core.windows.net/landing'

-- COMMAND ----------

-- DBTITLE 1,Cell 10
LIST '/Volumes/formula1_incr/landing/files';

-- COMMAND ----------

