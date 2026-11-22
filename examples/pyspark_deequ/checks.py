"""Same rules on Spark (Databricks, EMR, Glue) with PyDeequ.

pip install pydeequ   # and set SPARK_VERSION to match your cluster, e.g. export SPARK_VERSION=3.5
Results are written to a Delta/Parquet table so you keep history, just like dq.dq_result.
"""
import os

os.environ.setdefault("SPARK_VERSION", "3.5")

import pydeequ
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .config("spark.jars.packages", pydeequ.deequ_maven_coord)
         .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
         .getOrCreate())

stg = spark.table("stg_program_records")
tgt = spark.table("tgt_program_records")
dim = spark.table("dim_program")

# 1. Rule checks (critical = Error level, warn = Warning level)
critical = (Check(spark, CheckLevel.Error, "critical rules")
            .isComplete("record_id")
            .isUnique("record_id")
            .isComplete("program_id")
            .isComplete("category"))          # NULL = value the mapping didn't cover

warn = (Check(spark, CheckLevel.Warning, "warn rules")
        .isContainedIn("status", ["open", "closed", "pending"])
        .isNonNegative("amount"))

result = VerificationSuite(spark).onData(tgt).addCheck(critical).addCheck(warn).run()
results_df = (VerificationResult.checkResultsAsDataFrame(spark, result)
              .withColumn("checked_at", F.current_timestamp()))
results_df.write.mode("append").saveAsTable("dq.dq_result_spark")

# 2. Reconciliation (Deequ checks one table at a time; compare tables with joins)
lost = stg.join(tgt, "record_id", "left_anti").count()
orphans = tgt.join(dim, "program_id", "left_anti").filter(F.col("program_id").isNotNull()).count()
dupes = tgt.groupBy("record_id").count().filter("count > 1").count()

print(f"lost={lost} orphans={orphans} duplicates={dupes}")

# 3. Gate: stop the job if anything critical failed
if result.status != "Success" or lost or orphans or dupes:
    raise RuntimeError("Critical data quality checks failed: batch stopped. See dq.dq_result_spark.")
