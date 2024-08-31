from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark import SparkContext
import os
import sys

SPARK_APPLICATION_ARGS = "scf-lakehouse/SeeClickFix_Raw/issues.json"

def initialize_spark(app_name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("fs.s3a.access.key", "minio") \
        .config("fs.s3a.secret.key", "minio456") \
        .config("fs.s3a.endpoint", "http://172.19.0.5:9000") \
        .config("fs.s3a.connection.ssl.enabled", "false") \
        .config("fs.s3a.path.style.access", "true") \
        .config("fs.s3a.attempts.maximum", "1") \
        .config("fs.s3a.connection.establish.timeout", "5000") \
        .config("fs.s3a.connection.timeout", "10000") \
        .getOrCreate()


def load_data(spark: SparkSession, file_path: str) -> DataFrame:
    return spark.read.json(file_path)

# Select columns
def select_columns(df: DataFrame) -> DataFrame:
    return df.select(
        col("id"),
        col("reporter.name").alias("reporter_name"),
        col("created_at"),
        col("updated_at"),
        col("status"),
        col("summary"),
        col("description"),
        col("lat").alias("latitude"),
        col("lng").alias("longitude"),
        col("address").alias("issue_address"),
        col("url").alias("issue_url"),
        col("reporter.role").alias("reporter_role"),
        col("request_type.organization").alias("organization"),
        col("request_type.title").alias("title"),
        col("private_visibility").alias("is_private"),
        col("rating")
    )

# Rename columns
def rename_columns(df: DataFrame) -> DataFrame:
    r_dict = {
        "created_at": "issue_created_at",
        "updated_at": "issue_updated_at",
    }

    for old_name, new_name in r_dict.items():
        df = df.withColumnRenamed(old_name, new_name)
    
    return df

def convert_data_types(df: DataFrame) -> DataFrame:
    return df.withColumn("issue_created_at", to_timestamp(col("issue_created_at"))) \
             .withColumn("issue_updated_at", to_timestamp(col("issue_updated_at"))) \
             .withColumn("Date", to_date(col("issue_created_at")))

# transformation function
def transform_data(spark: SparkSession, file_path: str) -> DataFrame:
    df = load_data(spark, file_path)
    df = select_columns(df)
    df = rename_columns(df)
    df = convert_data_types(df)
    df.printSchema()
    return df

def save_to_csv(df: DataFrame, output_path: str):
    df.write.csv(output_path, header=True, mode="overwrite")

if __name__ == "__main__":

    issues_transformed = "issues_transformed.csv"
    file_path ="s3a://scf-lakehouse/SeeClickFix_Raw/issues.json"
    output_path = f"s3a://scf-lakehouse/SeeClickFix_Raw/{issues_transformed}"

    spark = initialize_spark("Transformation Script")
    
    transformed_df = transform_data(spark, file_path)
    
    save_to_csv(transformed_df, output_path)
    
    spark.stop()

    sys.exit()
