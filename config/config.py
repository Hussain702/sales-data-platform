# config/config.py

# Central Configuration — Sales Data Warehouse


from datetime import datetime



# Environment

ENV = "dev"
BATCH_ID=datetime.now().strftime("%Y%m%d%H%M%S")

# ADLS GEN2 ACCESS
storage_account = "salessource"

container = "sales-source"
# Databricks / DBFS Paths

BASE_PATH = "salesdw"                    
SOURCE_PATH = f"/Volumes/sales_dw/source/source_data"
BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"
GOLD_PATH   = f"{BASE_PATH}/gold"
VOLUME="bronze_data"
CHECKPOINT_PATH = f"/Volumes/sales_dw/bronze/bronze_data/checkpoints"
LOG_PATH    = f"Volumes/sales_dw/bronze/bronze_data/logs"
#Catalogue Name
CATALOG_NAME = "salesdw"

# Database / Schema Names

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"


# Bronze Table Names

BRONZE_CUSTOMERS = f"{BRONZE_SCHEMA}.bronze_customers"
BRONZE_PRODUCTS  = f"{BRONZE_SCHEMA}.bronze_products"
BRONZE_ORDERS    = f"{BRONZE_SCHEMA}.bronze_orders"


# Silver Table Names

SILVER_CUSTOMERS = f"{SILVER_SCHEMA}.customers"
SILVER_PRODUCTS  = f"{SILVER_SCHEMA}.products"
SILVER_ORDERS    = f"{SILVER_SCHEMA}.orders"


# Gold Table Names

DIM_CUSTOMER    = f"{GOLD_SCHEMA}.dim_customer"
DIM_PRODUCT     = f"{GOLD_SCHEMA}.dim_product"
DIM_DATE        = f"{GOLD_SCHEMA}.dim_date"
DIM_GEOGRAPHY   = f"{GOLD_SCHEMA}.dim_geography"
FACT_SALES      = f"{GOLD_SCHEMA}.fact_sales"


# Date Dimension Config

DIM_DATE_START = "2020-01-01"
DIM_DATE_END   = "2030-12-31"


# Spark / Delta Config

SPARK_CONFIG = {
    "spark.sql.extensions":                  "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog":       "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.shuffle.partitions":          "200",
    "spark.sql.adaptive.enabled":            "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.databricks.delta.optimizeWrite.enabled":  "true",
    "spark.databricks.delta.autoCompact.enabled":    "true",
}


# Data Quality Thresholds

DQ_NULL_THRESHOLD       = 0.05   # Max 5% nulls allowed
DQ_DUPLICATE_THRESHOLD  = 0.01   # Max 1% duplicates allowed
DQ_MIN_ROW_COUNT        = 1      # Minimum rows expected
