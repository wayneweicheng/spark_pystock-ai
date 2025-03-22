import os
import sys
from src.read_data import create_spark_session, read_stock_data
from src.transform_data import group_by_hour, write_results

# Define constants for paths (makes it easier to patch for tests)
INPUT_PATH = (
    "src/data/TransformStockTickSaleVsBidAsk_AX_20240814_20241123222717.parquet"
)
OUTPUT_PATH = "src/result/output.csv"


def main():
    try:
        # Create a Spark session
        spark = create_spark_session()

        # Debug: Print actual paths being used
        print(f"ACTUAL INPUT_PATH: {INPUT_PATH}")
        print(f"ACTUAL OUTPUT_PATH: {OUTPUT_PATH}")

        print("Reading parquet file...")
        # Read the parquet file
        df = read_stock_data(spark, INPUT_PATH, verbose=True)

        print("Performing aggregation by hour...")
        # Group by hour and calculate aggregates
        result_df = group_by_hour(df, verbose=True)

        # Ensure the output directory exists
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

        # Write results to CSV
        write_results(result_df, OUTPUT_PATH, verbose=True)

        # Stop the Spark session
        spark.stop()

    except Exception as e:
        print(f"Error: {e}")
        print("\nInstructions:")
        print("1. Make sure PySpark is installed: pip install pyspark")
        print("2. Run this script with: python src/transform.py")
        sys.exit(1)


if __name__ == "__main__":
    main()
