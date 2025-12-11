from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, lower, to_date, date_format,
    sum as _sum, rank,
    max as _max, datediff, lit
)

spark = SparkSession.builder.appName("OrdersSimpleVerbose").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------------
print("\n********** STEP 1: READING DATA **********\n")
# ------------------------------------------------------------------
input_path = r"hdfs:///user/ec2-user/UKUS18nov/spandy/orders.csv"

df_raw = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(input_path))

print("Rows loaded:", df_raw.count())


# ------------------------------------------------------------------
print("\n********** STEP 2: PREPROCESSING **********\n")
# ------------------------------------------------------------------

# --- Select + cast types + clean text ---
df = (df_raw
      .select(
          col("order_id").cast("int"),
          col("order_customer_id").cast("int"),
          col("sum").cast("double"),
          to_date(col("order_date"), "dd-MM-yyyy").alias("order_date"),
          lower(trim(col("order_status"))).alias("order_status")
      )
)

print("After selecting and casting:", df.count())

# --- Drop duplicates ---
before = df.count()
df = df.dropDuplicates()
after = df.count()
print("Duplicates removed:", before - after)

# --- Drop nulls in required columns ---
before = df.count()
df = df.na.drop(subset=["order_id", "order_customer_id", "sum", "order_date", "order_status"])
after = df.count()
print("Null rows removed:", before - after)

# --- Remove invalid rows ---
before = df.count()
df = df.filter(
    (col("order_id") > 0) &
    (col("order_customer_id") > 0) &
    (col("sum") >= 0)
)
after = df.count()
print("Invalid rows removed:", before - after)

print("\nSchema after preprocessing:")
df.printSchema()
df.show(5, truncate=False)


# ------------------------------------------------------------------
print("\n********** Q1 – CUSTOMER RANKING BASED ON TOTAL REVENUE **********\n")
# ------------------------------------------------------------------
cust_revenue = (df
    .groupBy("order_customer_id")
    .agg(_sum("sum").alias("total_revenue"))
)

print("Revenue rows:", cust_revenue.count())

w = Window.orderBy(col("total_revenue").desc())
customer_rank = cust_revenue.withColumn("rank", rank().over(w))

print("Top customers by revenue:")
customer_rank.show(10)


# ------------------------------------------------------------------
print("\n********** Q2 – PIVOT ORDER STATUS INTO COLUMNS **********\n")
# ------------------------------------------------------------------
status_pivot = (df
    .groupBy("order_customer_id")
    .pivot("order_status")
    .agg(_sum("sum"))
)

print("Pivot result rows:", status_pivot.count())
status_pivot.show(10)


# ------------------------------------------------------------------
print("\n********** Q3 – MONTHLY REVENUE TRENDS **********\n")
# ------------------------------------------------------------------
monthly_revenue = (df
    .withColumn("year_month", date_format(col("order_date"), "yyyy-MM"))
    .groupBy("year_month")
    .agg(_sum("sum").alias("monthly_revenue"))
    .orderBy("year_month")
)

print("Monthly revenue rows:", monthly_revenue.count())
monthly_revenue.show(20)


# ------------------------------------------------------------------
print("\n********** Q4 – IDENTIFY CHURNED CUSTOMERS **********\n")
# ------------------------------------------------------------------
churn_days = 10

# global last order date
last_order_date_value = (
    df.agg(_max("order_date").alias("max_date")).first()["max_date"]
)

print("Latest order date in dataset:", last_order_date_value)

# last order per customer
customer_last_order = (df
    .groupBy("order_customer_id")
    .agg(_max("order_date").alias("last_order_date"))
)

# days since last purchase
customer_churn = (customer_last_order
    .withColumn("days_since_last_order",
                datediff(lit(last_order_date_value), col("last_order_date")))
    .filter(col("days_since_last_order") > churn_days)
)

print("Churned customers (> {} days inactive): {}".format(churn_days, customer_churn.count()))

customer_churn.show(20)

print("\n********** ALL STEPS COMPLETED **********\n")
