#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, to_date

# SparkSession
def initialize_spark(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

# load JSON data
def load_data(spark: SparkSession, file_path: str) -> DataFrame:
    return spark.read.json(file_path)

# select relevant columns
def select_columns(df: DataFrame) -> DataFrame:
    return df.select(
        col("id"),
        col("reporter.name"),
        col("created_at"),
        col("updated_at"),
        col("status"),
        col("summary"),
        col("description"),
        col("lat"),
        col("lng"),
        col("address"),
        col("url"),
        col("reporter.role"),
        col("request_type.organization"),
        col("request_type.title"),
        col("private_visibility"),
        col("rating")
    )

# rename columns
def rename_columns(df: DataFrame) -> DataFrame:
    r_dict = {
        "id": "id",
        "name": "reporter_name",
        "created_at": "issue_created_at",
        "updated_at": "issue_updated_at",
        "status": "issue_status",
        "summary": "issue_summary",
        "description": "issue_description",
        "lat": "latitude",
        "lng": "longitude",
        "address": "issue_address",
        "url": "issue_url",
        "role": "reporter_role",
        "organization": "organization",
        "title": "title",
        "private_visibility": "is_private",
        "rating": "rating"
    }

    for old_name, new_name in r_dict.items():
        df = df.withColumnRenamed(old_name, new_name)
    
    return df

# ronvert data types
def convert_data_types(df: DataFrame) -> DataFrame:
    return df.withColumn("issue_created_at", to_timestamp(col("issue_created_at"))) \
             .withColumn("issue_updated_at", to_timestamp(col("issue_updated_at"))) \
             .withColumn("Date", to_date(col("issue_created_at")))

# Main transformation function
def transform_data(spark: SparkSession, file_path: str) -> DataFrame:
    df = load_data(spark, file_path)
    df = select_columns(df)
    df = rename_columns(df)
    df = convert_data_types(df)
    df.printSchema()
    df = df.drop("reporter_url")
    return df


def save_to_csv(df: DataFrame, output_path: str):
    df.write.csv(output_path, header=True, mode="overwrite")


if __name__ == "__main__":
    spark = initialize_spark("Transformation Script")
    
   
    transformed_df = transform_data(spark, "issues.json")
    
   
    save_to_csv(transformed_df, "output/issues_transformed.csv")
    
   
    spark.stop()
