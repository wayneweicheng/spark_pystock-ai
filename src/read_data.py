from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col


def create_spark_session(app_name="Stock Tick Data Analysis", is_cloud=False):
    """
    Creates and returns a SparkSession configured for local or cloud execution.

    Args:
        app_name (str): Name of the Spark application
        is_cloud (bool): Whether the session is for cloud (GCP/AWS) execution

    Returns:
        SparkSession: Configured SparkSession instance
    """
    # Start with the builder and app name
    builder = SparkSession.builder.appName(app_name)
    
    if is_cloud:
        # Cloud-specific configuration
        builder = builder.config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    else:
        # Local-specific configuration
        builder = builder.master("local[*]")
        
    # Common configuration
    spark = (
        builder
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )

    # Apply configuration for timestamp handling
    spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

    return spark


def read_stock_data(spark, input_path, verbose=False):
    """
    Reads stock data from a parquet file and converts timestamp data.

    Args:
        spark (SparkSession): SparkSession instance
        input_path (str): Path to the parquet file (local or cloud storage)
        verbose (bool): Whether to print schema and sample data

    Returns:
        DataFrame: Spark DataFrame with the stock data
    """
    # Read the parquet file with schema merging enabled
    df = spark.read.option("mergeSchema", "true").parquet(input_path)

    if verbose:
        print("Successfully read parquet file with schema:")
        df.printSchema()
        print("Sample records:")
        df.show(5)

    # Convert the SaleDateTime column from nanoseconds timestamp to a proper timestamp
    # The value is in nanoseconds, so we need to divide by 1,000,000,000 to get seconds
    df = df.withColumn("SaleDateTime", from_unixtime(col("SaleDateTime") / 1000000000))

    if verbose:
        print("Schema after SaleDateTime conversion:")
        df.printSchema()
        print("Sample records after conversion:")
        df.show(5)

    return df
