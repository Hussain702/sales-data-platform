from pyspark.sql.functions import * 
from typing import Dict, Any,List
from utils.logger import get_logger


class DQchecks:
    def __init__(self,tbl_name:str):
        self.tbl_name=tbl_name
        self.checks=[]
        self.passed=true

    def add(self,check_name:str,passed:bool,details:str =""):
        status="passed" if passed else "failed"
        self.checks.append({"check":check_name,"status":status,"details":details})
        if not passed:
            self.passed = False
        logger.info(f"[DQ] {self.tbl_name} | {check_name}: {status} | {details}")

    def summary(self) -> str:
        lines = [f"\n{'='*60}", f"DQ Report — {self.tbl_name}", f"{'='*60}"]
        for c in self.checks:
            lines.append(f"  [{c['status']:4}] {c['check']}: {c['details']}")
        overall = "ALL PASSED" if self.passed else " FAILURES DETECTED"
        lines.append(f"\nOverall: {overall}\n{'='*60}\n")
        return "\n".join(lines)  



def check_row_count(df:DataFrame, dq:DQchecks)-> DQchecks:
    min_count=1
    count=df.count()
    passed=count>=min_count
    dq.add("row_count_check", passed, f"rows={count}, min={min_count}")
    return dq

def check_nulls(df:DataFrame, cols:list,dq:DQchecks)->DQchecks:
    threshold=0.05
    total = df.count()
    if total == 0:
        result.add("null_check", False, "No rows to check")
        return dq
    for col in cols:
        null_count=df.filter(col.isNull()).count()
        null_rate=null_count/total
        passed=null_rate<threshold
        dq.add(f"null_check_{col}",passed,f"null_rate={null_rate}, threshold={threshold}")
    return dq

def check_duplicates(df:DataFrame, cols:list,dq:DQchecks)->DQchecks:
    total = df.count()
    if total == 0:
        result.add("duplicate_check", False, "No rows to check")
        return dq
    for col in cols:
        dup_count=df.groupBy(col).count().filter(col("count")>1).count()
        dup_rate=dup_count/total
        passed=dup_rate==0
        dq.add(f"duplicate_check_{col}",passed,f"duplicate_rate={dup_rate}")
    return dq        


    





















def standard_checks(df:DataFrame,table_name:str, non_null_cols:list,dedup_cols:list)->DQchecks:
    dq=DQchecks(table_name)
    check_row_count(df,dq)
    check_nulls(df,non_null_cols,dq)
    check_duplicates(df,dedup_cols,dq)
    print(result.summary())
    return result


