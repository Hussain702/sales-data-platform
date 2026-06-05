# notebooks/00_setup.py

import sys
import os

sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.getcwd(), ".."))
)

from utils.logger import get_logger
from config.config import (
    CATALOG_NAME,
    BRONZE_SCHEMA,
    SILVER_SCHEMA,
    GOLD_SCHEMA,
    VOLUME,
    storage_account,
    container,
)
logger = get_logger("00_setup")

# Connectng with azure data lake


spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
    "OAuth"
)

spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope = "test-scope",key="app-id")
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope = "test-scope",key="app-secret")
)

spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='test-scope', key='tenant-id')}/oauth2/v2.0/token"
)

dbutils.fs.ls(
    "abfss://sales-source@salessource.dfs.core.windows.net/"
)







external_location_name = "databricks_sales_dev"
external_location_url = spark.sql(
    f"DESCRIBE EXTERNAL LOCATION `{external_location_name}`"
).collect()[0]["url"]
catalog_managed_location = (
    f"{external_location_url.rstrip('/')}/{CATALOG_NAME}"
)

# ============================================================
# 1. Create Catalog
# ============================================================
logger.info("Creating catalog...")
logger.info(f"Using managed location: {catalog_managed_location}")

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS `{CATALOG_NAME}`
MANAGED LOCATION '{catalog_managed_location}'
""")

logger.info(f"Catalog ready: {CATALOG_NAME}")

# ============================================================
# 2. Create Schemas
# ============================================================
logger.info("Creating Bronze, Silver and Gold schemas...")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS `{CATALOG_NAME}`.`{BRONZE_SCHEMA}`
COMMENT 'Raw ingestion layer'
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS `{CATALOG_NAME}`.`{SILVER_SCHEMA}`
COMMENT 'Cleansed and conformed layer'
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS `{CATALOG_NAME}`.`{GOLD_SCHEMA}`
COMMENT 'Business-ready layer'
""")

logger.info("Schemas created successfully.")

# ============================================================
# 3. Create Volume
# ============================================================
logger.info("Creating volume...")

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS
`{CATALOG_NAME}`.`{BRONZE_SCHEMA}`.`{VOLUME}`
""")

logger.info("Volume created successfully.")

# ============================================================
# 4. Create Folders Inside Volume
# ============================================================

volume_path = (
    f"/Volumes/{CATALOG_NAME}/{BRONZE_SCHEMA}/{VOLUME}"
)

dbutils.fs.mkdirs(f"{volume_path}/checkpoints")
dbutils.fs.mkdirs(f"{volume_path}/logs")
dbutils.fs.mkdirs(f"{volume_path}/raw")

logger.info("Volume directories created successfully.")

# ============================================================
# 5. Azure Data Lake Connection (CORRECTED PART)
# ============================================================

logger.info("Configuring Azure Data Lake connection...")

try:
    # Serverless compute does not allow setting fs.azure.account.key.* configs.
    # Validate storage access through the registered Unity Catalog external location instead.
    folderPath = external_location_url

    logger.info("Using Unity Catalog external location for storage access.")

    # Simple validation (beginner-safe check)
    test_path = folderPath

    logger.info(f"Testing path: {test_path}")

    dbutils.fs.ls(test_path)

    logger.info(" Azure Data Lake connection SUCCESSFUL")

except Exception as e:
    logger.error(" Azure Data Lake connection FAILED")
    logger.error(str(e))
    raise e

# ============================================================
# 6. Verify Setup
# ============================================================

schemas = [
    row.databaseName
    for row in spark.sql(
        f"SHOW SCHEMAS IN `{CATALOG_NAME}`"
    ).collect()
]

logger.info(f"Schemas found: {schemas}")

assert BRONZE_SCHEMA in schemas, f"{BRONZE_SCHEMA} not found"
assert SILVER_SCHEMA in schemas, f"{SILVER_SCHEMA} not found"
assert GOLD_SCHEMA in schemas, f"{GOLD_SCHEMA} not found"

logger.info("=" * 50)
logger.info("Environment setup completed successfully")
logger.info(f"Catalog : {CATALOG_NAME}")
logger.info(f"Bronze  : {BRONZE_SCHEMA}")
logger.info(f"Silver  : {SILVER_SCHEMA}")
logger.info(f"Gold    : {GOLD_SCHEMA}")
logger.info(f"Volume  : {VOLUME}")
logger.info("=" * 50)