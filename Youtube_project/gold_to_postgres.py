from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("Gold_to_Postgres") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

GOLD_PATH = "/home/ec2-user/ukus18nov/spandy/gold"

jdbc_url = "jdbc:postgresql://18.134.163.221:5432/testdb"
jdbc_properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

tables = {
    "channel_performance": "spandana.gold_channel_performance",
    "category_performance": "spandana.gold_category_performance",
    "monthly_trending": "spandana.gold_monthly_trending",
    "overall_kpis": "spandana.gold_overall_kpis",
    "channel_country": "spandana.gold_channel_country"
}

for gold_table, pg_table in tables.items():
    path = os.path.join(GOLD_PATH, gold_table)

    if not os.path.exists(path):
        print("SKIPPED (missing):", gold_table)
        continue

    print("Writing:", gold_table, "->", pg_table)

    df = spark.read.parquet(path)

    df.write \
        .mode("overwrite") \
        .jdbc(
            url=jdbc_url,
            table=pg_table,
            properties=jdbc_properties
        )

print("Gold data successfully written to Postgres")

spark.stop()
