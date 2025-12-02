# Databricks notebook source
# MAGIC %md
# MAGIC # **Wet File Data Extraction**
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Import libraries 

# COMMAND ----------

from pyspark.sql import functions as F
import boto3
import botocore
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Connect to Boto3 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Set Secrets

# COMMAND ----------

aws_access_key_id = dbutils.secrets.get(scope='aws_cc', key='aws_access_key_id')
aws_secret_access_key = dbutils.secrets.get(scope='aws_cc', key='aws_secret_access_key')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Intialize boto3 client

# COMMAND ----------

# Optional: build client once (faster)
s3 = boto3.client(
    "s3",
    region_name="us-east-1",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read Bronze Layer
# MAGIC - Read in all crawls from bronze
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F


# Read the raw crawls table and filter for keys containing 'wet'
df_all_crawls = (
    spark.read.table("census_bureau_capstone.bronze.raw_all_crawls")
    .filter(F.col("key").contains("wet"))
)

# Extract crawl start and end timestamps from the 'key' column
df_all_crawls = df_all_crawls.withColumn(
    "crawl_start_raw",
    F.regexp_extract(F.col("key"), r"CC-MAIN-(\d{14})-(\d{14})-", 1)
).withColumn(
    "crawl_end_raw",
    F.regexp_extract(F.col("key"), r"CC-MAIN-(\d{14})-(\d{14})-", 2)
)

# Convert extracted timestamp strings to timestamp type
df_all_crawls = df_all_crawls.withColumn(
    "crawl_start",
    F.expr("try_to_timestamp(crawl_start_raw, 'yyyyMMddHHmmss')")
).withColumn(
    "crawl_end",
    F.expr("try_to_timestamp(crawl_end_raw, 'yyyyMMddHHmmss')")
).orderBy("crawl_start")

# Filter out rows where crawl_start could not be parsed
df_all_crawls = df_all_crawls.filter(F.col("crawl_start").isNotNull())

# Drop unnecessary columns
df_all_crawls = df_all_crawls.drop("crawl_start_raw", "crawl_end_raw", "ChecksumAlgorithm")

df_sample_size_crawls = spark.read.table("census_bureau_capstone.bronze.sample_size_raw")

df_all_crawls = df_all_crawls.join(
    df_sample_size_crawls.select("key"),
    on="key",
    how="anti"
)

# Count total number of rows
total = df_all_crawls.count()

print(f"total: {total:,}")
display(df_all_crawls)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Sort crawls
# MAGIC - random sample from past 5 years for each
# MAGIC - limit to 1 sample for each year
# MAGIC - set to python list

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1.1 Sample of data 

# COMMAND ----------

from pyspark.sql import functions as F

years = ['2021', '2022', '2023', '2024', '2025']
samples = []

def sample_crawls(years, df):
    samples = []
    for year in years:
        sample = (
            df.filter(F.col("crawl_start").cast("string").contains(year))
            .orderBy("crawl_start")
            .limit(2)
        )
        if sample.count() > 0:
            samples.append(sample)
    if samples:
        df_out = samples[0]
        for sample in samples[1:]:
            df_out = df_out.union(sample)
        df_out = df_out.toPandas()
        return df_out
    else:
        print("No samples found for the given criteria.")

df_all_crawls_samples = sample_crawls(years, df_all_crawls)


# COMMAND ----------

display(df_all_crawls_samples)
df_all_crawls_samples_spark = spark.createDataFrame(df_all_crawls_samples)

total_files = df_all_crawls_samples.count()

print(f"Total files ingested: {total_files}")
df_all_crawls_samples_spark.write.mode("append").saveAsTable("census_bureau_capstone.bronze.sample_size_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1.2 Set to python list 

# COMMAND ----------

key_list = df_all_crawls_samples['Key'].tolist()

print(key_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. NLP

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Extract CC Wet file
# MAGIC - Extracrts the random sample text files

# COMMAND ----------

source_bucket = "commoncrawl"
destination_bucket = 'mydbxbucketpractice'


def download_and_upload(source_key, destination_key):
    for source_key in key_list:
        destination_key = (
            'common_crawl/wet_files/' +
            source_key.split("/")[-1]
        )
        local_filename = '/tmp/' + source_key.split("/")[-1]
        
        s3.download_file(source_bucket, source_key, local_filename)
        s3.upload_file(local_filename, destination_bucket, destination_key)
        os.remove(local_filename)
        print(
            f"Copied s3://{source_bucket}/{source_key} to "
            f"s3://{destination_bucket}/{destination_key}"
        )

download_and_upload(source_bucket, destination_bucket)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2. View raw file as df

# COMMAND ----------

file_paths = [f"s3://{destination_bucket}/common_crawl/wet_files/{file.name}" for file in dbutils.fs.ls(f"s3://{destination_bucket}/common_crawl/wet_files/")]


df = spark.read.text(file_paths)
display(df)
df = df

total_records_processed = df.count()

print(f"total: {total_records_processed:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.2.1 Drop Files if needed 

# COMMAND ----------

# Drop all files from the directory
# for file_path in file_paths:
   # dbutils.fs.rm(file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3. Text transformation