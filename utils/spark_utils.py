from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from config.config import BATCH_ID
from utils.logger import get_logger
import sys ,os
logger=get_logger('spark utilities')
sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.getcwd(), ".."))
)
def add_audit_columns(df: DataFrame, source_system: str,
                      batch_id: str = BATCH_ID) -> DataFrame:
   
    
    return (
        df
        .withColumn("ingested_at",current_timestamp())
        .withColumn("source_system",lit(source_system))
        .withColumn("batch_id",lit(batch_id))
    )
def add_silver_audit_columns(df: DataFrame) -> DataFrame:
   
    
    return (
        df
        .withColumn("created_at",current_timestamp())
        .withColumn("updated_at",current_timestamp())
        
        
    )

def write_delta_overwrite(df:DataFrame, path:str,partition_by: list=None)->None:
    
      
    logger.info(f"Writing (overwrite) → {path}")

    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(path)

    logger.info(f"Write complete → {path} | rows: {df.count()}")
    
def write_delta_append(df:DataFrame, path:str,partition_by:list=None)->None:
    logger.info(f"Writing (overwrite) → {path}")

    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(path)

    logger.info(f"Write complete → {path} | rows: {df.count()}")


# ================================================================
# SILVER — TRANSFORMATIONS
# ================================================================
## deduplication of records

def dedup(df:DataFrame,col_list:list,reg_date:str)-> DataFrame:
    df = df.withColumn("dedup_key",concat(*col_list))
    df = df.withColumn("dedup_counts",row_number().over(Window.partitionBy    ("dedup_key").orderBy(col(reg_date).desc())))
    df = df.filter(col("dedup_counts")==1)
    df = df.drop("dedup_key","dedup_counts")
    return df
## droping null columns    
def drop_nulls(df: DataFrame, col_list: list) -> DataFrame:
    return df.dropna(subset=col_list)


    
   