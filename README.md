# Databricks_End_to_End_Project_Formula1
Completed an End-to-End Data Engineering Project using Databricks, PySpark, Delta Lake, Unity Catalog, and Azure Data Lake Storage Gen2.  Built scalable Bronze-Silver-Gold ETL pipelines with incremental loading, MERGE operations, audit tracking, and analytical Gold layer reporting using Formula 1 racing data.

## Built an end-to-end Data Engineering project using Databricks, PySpark, Delta Lake, and Unity Catalog based on Formula 1 racing data.

Implemented a modern Medallion Architecture (Bronze, Silver, Gold) with incremental data ingestion, transformation, and analytics pipelines. Designed reusable ETL frameworks for schema evolution, merge/upsert operations, audit tracking, and batch processing.

Key Highlights:\
• Developed scalable ETL pipelines using PySpark and Spark SQL\
• Implemented Delta Lake MERGE operations for incremental loading\
• Built reusable utility functions for Silver and Gold layer processing\
• Used Unity Catalog for centralized governance and data management\ 
• Processed CSV data from Azure Data Lake Storage Gen2 (ADLS Gen2)\
• Created dimensional and fact tables for analytical reporting \
• Applied data quality checks, ingestion metadata, and audit columns\
• Used Databricks Workflows and widgets for parameterized execution\
• Designed star-schema style Gold layer for business analytics\

Tech Stack:
Databricks, PySpark, Spark SQL, Delta Lake, Unity Catalog, Azure Data Lake Storage Gen2 (ADLS Gen2), Python

## Additionally, the solution included real-world data engineering concepts such as:

The project also implemented batch processing controls, ingestion tracking, and audit columns such as created and updated timestamps to support enterprise-grade monitoring and traceability. Unity Catalog was used for centralized governance, schema management, and secure access control across catalogs and schemas. Parameterized notebook execution was achieved using Databricks widgets and workflows, enabling reusable and automated pipeline orchestration.

Incremental data loading\
Schema evolution handling\
Delta Lake ACID transactions\
Merge and upsert frameworks\
Metadata-driven processing\
Reusable ETL utility modules\
Analytical Gold layer modeling\
Data quality validation\
Batch status tracking\

This project strengthened hands-on experience in building scalable cloud-based data platforms and modern lakehouse architectures while applying industry best practices used in enterprise data engineering environments.
