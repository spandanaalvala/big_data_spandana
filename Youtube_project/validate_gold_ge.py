from pyspark.sql import SparkSession
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
import os

spark = SparkSession.builder \
    .appName("GE_Gold_Validation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

GOLD_PATH = "/home/ec2-user/ukus18nov/spandy/gold"

tables = {
    "channel_performance": {
        "path": "channel_performance",
        "not_null": ["channel_title", "country"],
        "unique": ["channel_title", "country"]
    },
    "category_performance": {
        "path": "category_performance",
        "not_null": ["category_id", "country"],
        "unique": ["category_id", "country"]
    },
    "monthly_trending": {
        "path": "monthly_trending",
        "not_null": ["year", "month", "country"],
        "unique": ["year", "month", "country"]
    },
    "overall_kpis": {
        "path": "overall_kpis",
        "not_null": ["country"],
        "unique": ["country"]
    },
    "channel_country": {
        "path": "channel_country",
        "not_null": ["channel_title", "country"],
        "unique": ["channel_title", "country"]
    }
}

failed = []

for table, rules in tables.items():
    path = os.path.join(GOLD_PATH, rules["path"])

    if not os.path.exists(path):
        print("SKIPPED (missing):", table)
        continue

    df = spark.read.parquet(path)
    ge_df = SparkDFDataset(df)

    print("Validating:", table)


    # Row count
#    ge_df.expect_table_row_count_to_be_greater_than(0)
    ge_df.expect_table_row_count_to_be_between(min_value=1)

    # Not null checks
    for col in rules["not_null"]:
        ge_df.expect_column_values_to_not_be_null(col)

    # Uniqueness check
    ge_df.expect_compound_columns_to_be_unique(rules["unique"])

    result = ge_df.validate()

    if result["success"]:
        print("PASSED:", table)
    else:
        print("FAILED:", table)
        failed.append(table)

spark.stop()

if failed:
    raise Exception("Gold validation failed for: " + ", ".join(failed))
