from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# -------------------------
# Spark Session
# -------------------------
spark = SparkSession.builder \
    .appName("FinancialStreamProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------
# Schema: Market Data
# -------------------------
market_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("timestamp", StringType())
])

market_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "market_prices") \
    .load()

market_parsed = market_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), market_schema).alias("data")) \
    .select("data.*")

# -------------------------
# Schema: Trade CDC Data
# -------------------------
trade_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),   # entry price
    StructField("quantity", DoubleType())
])

trade_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "fintech.public.trades") \
    .load()

trade_parsed = trade_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), trade_schema).alias("data")) \
    .select("data.*")

# -------------------------
# Latest Market Price (Correct way: windowing)
# -------------------------
latest_price = market_parsed \
    .withColumn("event_time", to_timestamp("timestamp")) \
    .groupBy(
        window(col("event_time"), "10 seconds"),
        col("symbol")
    ) \
    .agg(
        max("price").alias("latest_price")
    ) \
    .select("symbol", "latest_price")

# -------------------------
# Trade Exposure
# -------------------------
trade_exposure = trade_parsed \
    .groupBy("symbol") \
    .agg(
        sum(col("quantity") * col("price")).alias("total_invested"),
        sum("quantity").alias("total_qty")
    )

# -------------------------
# Join Streams
# -------------------------
joined = trade_exposure.join(latest_price, "symbol", "inner")

# -------------------------
# Correct PnL Logic
# -------------------------
pnl = joined.withColumn(
    "current_value",
    col("total_qty") * col("latest_price")
).withColumn(
    "profit_loss",
    col("current_value") - col("total_invested")
)

# -------------------------
# Output
# -------------------------
query = pnl.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()