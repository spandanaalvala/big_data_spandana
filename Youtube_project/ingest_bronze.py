from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import os

spark = SparkSession.builder \
    .appName("Postgres_to_Bronze") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("ERROR")

jdbc_url = "jdbc:postgresql://18.134.163.221:5432/testdb"

properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

tables = ["ca_videos", "jp_videos", "ru_videos", "us_videos"]
schema = "kavya"

bronze_base_path = "/home/ec2-user/ukus18nov/spandy/bronze"
checkpoint_base = "/home/ec2-user/ukus18nov/spandy/checkpoints"

BATCH_SIZE = 10000

for table in tables:

    checkpoint_file = checkpoint_base + "/" + table + ".txt"

    last_ts = None
    if os.path.exists(checkpoint_file):
        f = open(checkpoint_file, "r")
        last_ts = f.read().strip()
        f.close()

    query = "(SELECT * FROM " + schema + "." + table + ") AS t"

    df = spark.read.jdbc(
        url=jdbc_url,
        table=query,
        properties=properties
    )

    # Drop extra column for ca_videos
    if table == "ca_videos":
        if "updated_at" in df.columns:
            df = df.drop("updated_at")

    df = df.withColumn(
        "publish_ts",
        to_timestamp(col("publish_time"))
    )

    if last_ts:
        df = df.filter(col("publish_ts") > last_ts)

    batch_df = df.orderBy("publish_ts").limit(BATCH_SIZE)

    if batch_df.count() == 0:
        print('*'*50)
        print('Done')
        continue

    output_path = bronze_base_path + "/" + table
    batch_df.write.mode("append").parquet(output_path)

    max_ts = batch_df.agg({"publish_ts": "max"}).collect()[0][0]

    f = open(checkpoint_file, "w")
    f.write(str(max_ts))
    f.close()

spark.stop()
