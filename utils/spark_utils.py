from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from config.config import BATCH_ID
import sys ,os

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