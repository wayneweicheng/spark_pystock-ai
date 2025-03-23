from pyspark.sql.functions import hour, col, sum, max, min
import os


def group_by_hour(df, verbose=False):
    """
    Groups stock data by hour and calculates aggregates.

    Args:
        df (DataFrame): Spark DataFrame with stock data
        verbose (bool): Whether to print the result summary

    Returns:
        DataFrame: Aggregated DataFrame with hourly statistics
    """
    # Convert SaleDateTime to hour and group by it
    result_df = (
        df.withColumn("HourOfDay", hour(col("SaleDateTime")))
        .groupBy("HourOfDay")
        .agg(
            sum("SaleValue").alias("TotalSaleValue"),
            sum("Quantity").alias("TotalQuantity"),
            max("Price").alias("MaxPrice"),
            min("Price").alias("MinPrice"),
        )
    )

    if verbose:
        print("Result summary:")
        result_df.show(24)  # Show all hours (0-23)

    return result_df


def write_results(df, output_path, verbose=False):
    """
    Write the results DataFrame to a CSV file, supporting both local and cloud storage.

    Args:
        df (DataFrame): Spark DataFrame to write
        output_path (str): Path where to write the CSV file (local path or cloud URI)
        verbose (bool): Whether to print progress messages

    Returns:
        None
    """
    if verbose:
        print(f"Writing results to {output_path}...")

    # Check if this is a cloud storage path (GCS or S3)
    is_cloud_path = output_path.startswith("gs://") or output_path.startswith("s3://")
    
    if is_cloud_path:
        # For cloud storage, write directly using Spark's CSV writer
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    else:
        # For local paths, ensure the directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Write the DataFrame to CSV using pandas
        df.toPandas().to_csv(output_path, index=False)

    if verbose:
        print(f"Results written successfully to {output_path}")
