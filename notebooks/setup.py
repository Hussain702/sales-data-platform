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
)

logger = get_logger("00_setup")

# ============================================================
# 1. Create Catalog
# ============================================================
logger.info("Creating catalog...")

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS `{CATALOG_NAME}`
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
# 5. Verify Setup
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
