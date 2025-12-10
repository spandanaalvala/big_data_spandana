from pyspark.sql.functions import count
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, trim, avg, count, when, desc, to_date,lower
from pyspark.sql.functions import sum as _sum, rank, date_format

spark = SparkSession.builder.appName("OrdersCleaning").getOrCreate()

input_path  = r"C:\BIG DATA\Testing\orders.csv"

df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(input_path))
# Convert data types
df = (df
      .withColumn("order_id", col("order_id").cast("int"))
      .withColumn("order_customer_id", col("order_customer_id").cast("int"))
      .withColumn("sum", col("sum").cast("float"))
      .withColumn("order_date", to_date(col("order_date"), "dd-MM-yyyy"))
     )


print("Initial rows:", df.count())

df = df.select([
    lower(trim(col(c))).alias(c) if t == "string" else col(c)
    for c, t in df.dtypes
])

print("After trimming + lowercasing:", df.count())

# --- 2) Remove duplicates ---
before_dups = df.count()
df = df.dropDuplicates()
after_dups = df.count()

print("Before removing duplicates:", before_dups)
print("After removing duplicates:",  after_dups)
print("Duplicates removed:", before_dups - after_dups)

# --- 3) Handle missing values (fill) ---

numeric_cols = [c for c, t in df.dtypes if t in ("int", "double", "float", "long")]
string_cols  = [c for c, t in df.dtypes if t == "string"]

# Fill numeric columns with MEAN
mean_values = df.select([avg(col(c)).alias(c) for c in numeric_cols]).collect()[0].asDict()

# Fill string columns with MODE
mode_values = {}
for c in string_cols:
    mode_row = (df.groupBy(c)
                 .count()
                 .orderBy(desc("count"))
                 .first())
    if mode_row:
        mode_values[c] = mode_row[0]

# Combine and fill
fill_values = {}
fill_values.update(mean_values)
fill_values.update(mode_values)

before_fill = df.count()
df = df.fillna(fill_values)
after_fill = df.count()

print("Missing-value fill applied. Rows unchanged:", before_fill == after_fill)

print("Done")

before = df.count()

df = df.filter(
    (col("order_id") > 0) &
    (col("order_customer_id") > 0) &
    (col("sum") >= 0)
)

after = df.count()

print("Invalid rows removed:", before - after)

df.printSchema()
df.show(10, truncate=False)

# How can you rank customers based on total revenue?
cust_rev = df.groupBy("order_customer_id") \
             .agg(_sum(col("sum")).alias("total_revenue"))

# ranking (1 = highest revenue)
w = Window.orderBy(col("total_revenue").desc())

ranked = cust_rev.withColumn("rank", rank().over(w))
print("Ranked data")
ranked.show()   

# How do you pivot order status so each status becomes a column?
from pyspark.sql.functions import sum as _sum

pivot_df = (df
            .groupBy("order_customer_id")
            .pivot("order_status")
            .agg(_sum("sum")))

pivot_df.show()



monthly_rev = (df
    .withColumn("year_month", date_format(col("order_date"), "yyyy-MM"))
    .groupBy("year_month")
    .agg(_sum("sum").alias("monthly_revenue"))
    .orderBy("year_month")
)

monthly_rev.show()

