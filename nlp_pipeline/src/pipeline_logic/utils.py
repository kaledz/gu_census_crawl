import boto3
from pyspark.sql import functions as F


def get_s3_client(dbutils, scope="aws_cc"):
    """Get AWS S3 client using Databricks secrets."""
    aws_access_key_id = dbutils.secrets.get(scope=scope, key="aws_access_key_id")
    aws_secret_access_key = dbutils.secrets.get(scope=scope, key="aws_secret_access_key")
    
    return boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )


def read_wet_crawls(spark, exclude_table="census_bureau_capstone.bronze.sample_size_raw"):
    """Read WET crawls from bronze, excluding already sampled ones."""
    df = (
        spark.read.table("census_bureau_capstone.bronze.raw_all_crawls")
        .filter(F.col("key").contains("wet"))
    )
    
    df = df.withColumn("crawl_start_raw", F.regexp_extract(F.col("key"), r"CC-MAIN-(\d{14})-(\d{14})-", 1))
    df = df.withColumn("crawl_end_raw", F.regexp_extract(F.col("key"), r"CC-MAIN-(\d{14})-(\d{14})-", 2))
    df = df.withColumn("crawl_start", F.expr("try_to_timestamp(crawl_start_raw, 'yyyyMMddHHmmss')"))
    df = df.withColumn("crawl_end", F.expr("try_to_timestamp(crawl_end_raw, 'yyyyMMddHHmmss')"))
    df = df.orderBy("crawl_start")
    df = df.filter(F.col("crawl_start").isNotNull())
    df = df.drop("crawl_start_raw", "crawl_end_raw", "ChecksumAlgorithm")
    
    # Exclude already sampled keys
    df_exclude = spark.read.table(exclude_table)
    df = df.join(df_exclude.select("key"), on="key", how="anti")
    
    return df


def list_s3_files(dbutils, bucket, prefix):
    """List S3 file paths."""
    files = dbutils.fs.ls(f"s3://{bucket}/{prefix}")
    return [f"s3://{bucket}/{prefix}{f.name}" for f in files]


def read_text_files(spark, paths):
    """Read text files into Spark DataFrame."""
    return spark.read.text(paths)
