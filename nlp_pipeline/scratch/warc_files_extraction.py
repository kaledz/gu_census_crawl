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

df_all_crawls = spark.read.table("census_bureau_capstone.bronze.raw_all_crawls")
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
# MAGIC #### 3.1.1 Random Sample

# COMMAND ----------

from pyspark.sql import functions as F

years = ['2021', '2022', '2023', '2024', '2025']
samples = []

for year in years:
    # Filter the DataFrame for each year and 'wet' keyword, then take a single random sample
    sample = (df_all_crawls.where(
                (F.col('key').contains('warc.gz')) &
                (F.year(F.col('LastModified')) == year))
              ).sample(withReplacement=False, fraction=1.0).limit(1)
    
    # Check if the sample is not empty before appending
    if sample.count() > 0:
        samples.append(sample)

# Initialize the combined DataFrame with the first sample if samples list is not empty
if samples:
    df_all_crawls_samples = samples[0]

    # Union the rest of the samples into the combined DataFrame
    for sample in samples[1:]:
        df_all_crawls_samples = df_all_crawls_samples.union(sample)

    # Convert to pandas
    df_all_crawls_samples = df_all_crawls_samples.toPandas()
    display(df_all_crawls_samples)

else:
    print("No samples found for the given criteria.")

# COMMAND ----------

df_all_crawls_samples = df_all_crawls_samples.head(2)

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
# MAGIC ### 4.1 Extract CC Warc file
# MAGIC - Extracrts the random sample text files

# COMMAND ----------

source_bucket = "commoncrawl"
destination_bucket = 'mydbxbucketpractice'

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

# COMMAND ----------

df = spark.read.text("s3://mydbxbucketpractice/common_crawl/wet_files/CC-MAIN-20211026134839-20211026164839-00000.warc.gz")

display(df)