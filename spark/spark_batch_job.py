from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

# Initialize Spark
spark = SparkSession.builder \
    .appName("HistoricalLandValuation") \
    .get_builder_or_create()

# 1. Load massive historical dataset (e.g., 10 years of price data)
df = spark.read.csv("/opt/spark/data/historical_prices.csv", header=True, inferSchema=True)

# 2. Heavy Transformation: Calculate 5-year CAGR per Parcel
processed_df = df.groupBy("parcel_id").agg(
    avg("price_sqft").alias("avg_historical_price"),
    avg("volatility_index").alias("market_stability")
)

# 3. Write to Snowflake
# Note: Requires Snowflake-Spark connector
processed_df.write \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "HISTORICAL_MARKET_TRENDS") \
    .mode("overwrite") \
    .save()