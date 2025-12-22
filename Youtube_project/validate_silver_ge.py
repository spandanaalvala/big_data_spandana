from pyspark.sql import SparkSession
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
import os

spark = SparkSession.builder \
    .appName("GE_Silver_Validation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

silver_base = "/home/ec2-user/ukus18nov/spandy/silver"
tables = ["ca_videos", "jp_videos", "ru_videos", "us_videos"]

failed_tables = []

for table in tables:
    path = silver_base + "/" + table
    if not os.path.exists(path):
        continue

    df = spark.read.parquet(path)

    ge_df = SparkDFDataset(
        df,
        expectation_suite_name="silver_quality_suite"
    )

    result = ge_df.validate()

    if not result["success"]:
        failed_tables.append(table)
        print("? Validation FAILED for table:", table)
    else:
        print("? Validation PASSED for table:", table)

spark.stop()

if failed_tables:
    raise Exception("Data quality check failed for tables: " + ",".join(failed_tables))
