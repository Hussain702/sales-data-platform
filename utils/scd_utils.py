

from pyspark.sql import functions as *
from pyspark.sql.window import Window



def add_surrogate_key(df, business_key, sk_col):
  
    
    window = Window.orderBy(business_key)

    return df.withColumn(sk_col, F.row_number().over(window))

def scd_type1_merge(df,trg_tbl,key_cols,cdc):
    merge_condition="AND".join([f"src.{i}=trg.{i}" for i in key_cols])
    dlt_obj=DeltaTable.forName(spark,trg_tbl)
    dlt_obj.alias("trg").merge(
        df.alias("src"),
        merge_condition
    ).whenMatchedUpdateAll(condition=f"src.{cdc}>=trg.{cdc}")\
    .whenNotMatchedInsertAll()\
    .execute()
    