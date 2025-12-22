from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import os

spark = SparkSession.builder \
    .appName("Bronze_to_Silver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

bronze_base = "/home/ec2-user/ukus18nov/spandy/bronze"
silver_base = "/home/ec2-user/ukus18nov/spandy/silver"
checkpoint_base = "/home/ec2-user/ukus18nov/spandy/checkpoints_silver"

tables = ["ca_videos", "jp_videos", "ru_videos", "us_videos"]
BATCH_SIZE = 10000

for table in tables:

    bronze_path = bronze_base + "/" + table
    silver_path = silver_base + "/" + table
    checkpoint_file = checkpoint_base + "/" + table + ".txt"

    if not os.path.exists(bronze_path):
        continue

    last_ts = None
    if os.path.exists(checkpoint_file):
        f = open(checkpoint_file, "r")
        last_ts = f.read().strip()
        f.close()

    df = spark.read.parquet(bronze_path)

    # Incremental filter
    if last_ts:
        df = df.filter(col("publish_ts") > last_ts)

    df = df.orderBy("publish_ts").limit(BATCH_SIZE)

    if df.count() == 0:
        print("No new bronze data for table:", table)
        print('*'*100)
        continue

    # ---------------- CLEANING ----------------

    # 1. Remove duplicates
    df = df.dropDuplicates(["video_id"])

    # 2. Remove rows with ANY null
    df = df.dropna()

    # 3. Cast data types
    df = df \
        .withColumn("category_id", col("category_id").cast("int")) \
        .withColumn("views", col("views").cast("long")) \
        .withColumn("likes", col("likes").cast("long")) \
        .withColumn("dislikes", col("dislikes").cast("long")) \
        .withColumn("comment_count", col("comment_count").cast("long")) \
        .withColumn("comments_disabled", col("comments_disabled").cast("boolean")) \
        .withColumn("ratings_disabled", col("ratings_disabled").cast("boolean")) \
        .withColumn("video_error_or_removed", col("video_error_or_removed").cast("boolean")) \
        .withColumn("trending_date", to_date(col("trending_date"), "yy.dd.MM"))

    # Remove redundant raw column
    df = df.drop("publish_time")

    # ------------------------------------------

    df.write.mode("append").parquet(silver_path)

    max_ts = df.agg({"publish_ts": "max"}).collect()[0][0]

    f = open(checkpoint_file, "w")
    f.write(str(max_ts))
    f.close()

spark.stop()
