

from pyspark import SparkContext
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, to_date

import os
import sys



def initialize_spark(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minio")) \
        .config("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minio456")) \
        .config("fs.s3a.endpoint", os.getenv("ENDPOINT", "http://minio:9000")) \
        .config("fs.s3a.connection.ssl.enabled", "false") \
        .config("fs.s3a.path.style.access", "true") \
        .config("fs.s3a.attempts.maximum", "1") \
        .config("fs.s3a.connection.establish.timeout", "5000") \
        .config("fs.s3a.connection.timeout", "10000") \
        .getOrCreate()

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

    issues_transformed = "/clean_data/issues_transformed.csv"
    file_path = f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}"
    output_path = f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}/issues_transformed"



    spark = initialize_spark("Transformation Script")
    
   
    transformed_df = transform_data(spark, file_path)
    
   
    save_to_csv(transformed_df, output_path)
    
   
    spark.stop()