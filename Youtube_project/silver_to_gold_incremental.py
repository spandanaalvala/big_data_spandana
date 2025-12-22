import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    year,
    month,
    lit
)
import os

def safe_overwrite(df, final_path):
    tmp_path = final_path + "_tmp"

    df.write.mode("overwrite").parquet(tmp_path)

    if os.path.exists(final_path):
        shutil.rmtree(final_path)

    os.rename(tmp_path, final_path)


# -------------------------------------------------
# Spark
# -------------------------------------------------
spark = SparkSession.builder \
    .appName("Silver_to_Gold_True_Incremental") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# -------------------------------------------------
# Paths
# -------------------------------------------------
SILVER_PATH = "/home/ec2-user/ukus18nov/spandy/silver"
GOLD_PATH = "/home/ec2-user/ukus18nov/spandy/gold"
CHECKPOINT_PATH = "/home/ec2-user/ukus18nov/spandy/checkpoints_gold"

COUNTRIES = ["ca", "us", "jp", "ru"]

if not os.path.exists(CHECKPOINT_PATH):
    os.makedirs(CHECKPOINT_PATH)

# -------------------------------------------------
# 1. Read SILVER incrementally
# -------------------------------------------------
dfs = []

for country in COUNTRIES:
    silver_path = SILVER_PATH + "/" + country + "_videos"
    checkpoint_file = CHECKPOINT_PATH + "/" + country + ".txt"

    if not os.path.exists(silver_path):
        continue

    df = spark.read.parquet(silver_path)

    last_ts = None
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            last_ts = f.read().strip()

    if last_ts:
        df = df.filter(col("publish_ts") > last_ts)

    if df.count() == 0:
        continue

    df = df.withColumn("country", lit(country))
    dfs.append(df)

    max_ts = df.agg({"publish_ts": "max"}).collect()[0][0]
    with open(checkpoint_file, "w") as f:
        f.write(str(max_ts))

if not dfs:
    print("No new silver data. Gold not updated.")
    spark.stop()
    exit(0)

final_df = dfs[0]
for d in dfs[1:]:
    final_df = final_df.unionByName(d)

# -------------------------------------------------
# Helper: merge incremental gold
# -------------------------------------------------
def merge_gold(delta_df, gold_path, group_cols, agg_exprs):
    if os.path.exists(gold_path):
        existing = spark.read.parquet(gold_path)
        combined = existing.unionByName(delta_df)
    else:
        combined = delta_df

    return combined.groupBy(*group_cols).agg(*agg_exprs)

# -------------------------------------------------
# 2. GOLD TABLES
# -------------------------------------------------

# A. Channel Performance
delta_channel = final_df.groupBy(
    "channel_title", "country"
).agg(
    spark_sum("views").alias("total_views"),
    spark_sum("likes").alias("total_likes"),
    spark_sum("dislikes").alias("total_dislikes"),
    spark_sum("comment_count").alias("total_comments"),
    count("*").alias("video_count")
)

channel_performance = merge_gold(
    delta_channel,
    GOLD_PATH + "/channel_performance",
    ["channel_title", "country"],
    [
        spark_sum("total_views").alias("total_views"),
        spark_sum("total_likes").alias("total_likes"),
        spark_sum("total_dislikes").alias("total_dislikes"),
        spark_sum("total_comments").alias("total_comments"),
        spark_sum("video_count").alias("video_count")
    ]
)

# B. Category Performance
delta_category = final_df.groupBy(
    "category_id", "country"
).agg(
    spark_sum("views").alias("total_views"),
    count("*").alias("video_count")
)

category_performance = merge_gold(
    delta_category,
    GOLD_PATH + "/category_performance",
    ["category_id", "country"],
    [
        spark_sum("total_views").alias("total_views"),
        spark_sum("video_count").alias("video_count")
    ]
)

# C. Monthly Trending
delta_monthly = final_df.withColumn(
    "year", year(col("trending_date"))
).withColumn(
    "month", month(col("trending_date"))
).groupBy(
    "year", "month", "country"
).agg(
    spark_sum("views").alias("total_views"),
    count("*").alias("video_count")
)

monthly_trending = merge_gold(
    delta_monthly,
    GOLD_PATH + "/monthly_trending",
    ["year", "month", "country"],
    [
        spark_sum("total_views").alias("total_views"),
        spark_sum("video_count").alias("video_count")
    ]
)

# D. Overall KPIs
delta_kpis = final_df.groupBy(
    "country"
).agg(
    spark_sum("views").alias("total_views"),
    spark_sum("likes").alias("total_likes"),
    spark_sum("comment_count").alias("total_comments"),
    count("*").alias("total_videos")
)

overall_kpis = merge_gold(
    delta_kpis,
    GOLD_PATH + "/overall_kpis",
    ["country"],
    [
        spark_sum("total_views").alias("total_views"),
        spark_sum("total_likes").alias("total_likes"),
        spark_sum("total_comments").alias("total_comments"),
        spark_sum("total_videos").alias("total_videos")
    ]
)

# E. Channel vs Country
delta_channel_country = final_df.groupBy(
    "channel_title", "country"
).agg(
    spark_sum("views").alias("total_views")
)

channel_country = merge_gold(
    delta_channel_country,
    GOLD_PATH + "/channel_country",
    ["channel_title", "country"],
    [
        spark_sum("total_views").alias("total_views")
    ]
)

# -------------------------------------------------
# 3. WRITE GOLD
# -------------------------------------------------

safe_overwrite(channel_performance, GOLD_PATH + "/channel_performance")
safe_overwrite(category_performance, GOLD_PATH + "/category_performance")
safe_overwrite(monthly_trending, GOLD_PATH + "/monthly_trending")
safe_overwrite(overall_kpis, GOLD_PATH + "/overall_kpis")
safe_overwrite(channel_country, GOLD_PATH + "/channel_country")

# channel_performance.write.mode("overwrite").parquet(GOLD_PATH + "/channel_performance")
# category_performance.write.mode("overwrite").parquet(GOLD_PATH + "/category_performance")
# monthly_trending.write.mode("overwrite").parquet(GOLD_PATH + "/monthly_trending")
# overall_kpis.write.mode("overwrite").parquet(GOLD_PATH + "/overall_kpis")
# channel_country.write.mode("overwrite").parquet(GOLD_PATH + "/channel_country")

print("TRUE incremental GOLD update completed successfully")

spark.stop()
