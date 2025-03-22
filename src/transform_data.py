from pyspark.sql.functions import hour, col, sum, max, min


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
    Write the results DataFrame to a CSV file.

    Parameters:
    -----------
    df : pyspark.sql.DataFrame
        The DataFrame to write.
    output_path : str
        The path where to write the CSV file.
    verbose : bool, optional
        Whether to print progress messages, by default False.
    """
    if verbose:
        print(f"Writing results to {output_path}...")

    # Ensure the directory exists
    import os

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Write the DataFrame to CSV using pandas
    df.toPandas().to_csv(output_path, index=False)

    if verbose:
        print(f"Results written successfully to {output_path}")
