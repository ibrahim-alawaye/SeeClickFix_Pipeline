#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, to_date

# Initialize SparkSession
def initialize_spark(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

# Load JSON data
def load_data(spark: SparkSession, file_path: str) -> DataFrame:
    return spark.read.json(file_path)

# Select relevant columns
def select_columns(df: DataFrame) -> DataFrame:
    return df.select(
        col("id"),
        col("reporter.name").alias("reporter_name"),
        col("created_at"),
        col("updated_at"),
        col("status"),
        col("summary"),
        col("lat").alias("latitude"),
        col("lng").alias("longitude"),
        col("address").alias("issue_address"),
        col("url").alias("issue_url"),
        col("reporter.role").alias("reporter_role"),
        col("request_type.organization").alias("organization"),
        col("request_type.title").alias("title"),
        col("private_visibility").alias("is_private"),
        col("rating"),
    )

# Convert data types
def convert_data_types(df: DataFrame) -> DataFrame:
    return df.withColumn("issue_created_at", to_timestamp(col("created_at"))) \
             .withColumn("issue_updated_at", to_timestamp(col("updated_at"))) \
             .withColumn("Date", to_date(col("created_at")))

# Filter out rows with non-integer IDs
def drop_non_integer_rows(df: DataFrame) -> DataFrame:
    # Cast the 'id' column to integer and filter out rows where casting fails
    return df.filter(col('id').cast('int').isNotNull())

# Main transformation function
def transform_data(spark: SparkSession, file_path: str) -> DataFrame:
    df = load_data(spark, file_path)
    df = select_columns(df)
    df = convert_data_types(df)
    df = drop_non_integer_rows(df)
    df.printSchema()  # Optionally print the schema for verification
    return df

# Save DataFrame to CSV
def save_to_csv(df: DataFrame, output_path: str):
    df.write.csv(output_path, header=True, mode="overwrite")

if __name__ == "__main__":
    spark = initialize_spark("Transformation Script")
    
    # Transform the data
    transformed_df = transform_data(spark, "issues.json")
    
    # Save the transformed data to a CSV file
    save_to_csv(transformed_df, "output/issues_transformed.csv")
    
    # Stop the Spark session
    spark.stop()
