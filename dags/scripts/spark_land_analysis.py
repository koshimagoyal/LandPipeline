from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder \
    .appName("TerraAgent-Historical-Analysis") \
    .getOrCreate()

# Create dummy historical data (Simulating a large CSV)
data = [("P101", 2021, 450000), ("P101", 2022, 475000), ("P101", 2023, 500000)]
df = spark.createDataFrame(data, ["parcel_id", "year", "price"])

# Perform a calculation (e.g., Year-over-Year growth)
df.show()

print("🚀 Spark Analysis Complete for P101")
spark.stop()