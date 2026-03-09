from pyspark.sql import SparkSession
import sys

def main():
    spark = SparkSession.builder \
        .appName("TerraAgent-Batch") \
        .getOrCreate()

    # Reduce log noise so Airflow doesn't get overwhelmed
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("🚀 Processing Land Data...")
        data = [("P101", 2024, 1500000.0)]
        df = spark.createDataFrame(data, ["ID", "Year", "Price"])
        df.show()
        print("✅ Spark processing finished successfully")
    except Exception as e:
        print(f"❌ Spark Error: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()