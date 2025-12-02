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
                (F.col('key').contains('wat')) &
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

# MAGIC %md
# MAGIC ### 4.2. View raw file as df

# COMMAND ----------

from pyspark.sql.functions import col, from_json

file_paths = [f"s3://{destination_bucket}/common_crawl/wet_files/{file.name}" for file in dbutils.fs.ls(f"s3://{destination_bucket}/common_crawl/wet_files/")]

df_combined = spark.read.text(file_paths)

# Filter rows that look like JSON objects (start with '{' and end with '}')
df_json = df_combined.filter(
    (col("value").startswith("{")) & (col("value").endswith("}"))
)

schema = StructType([
    StructField("Container", StructType([
        StructField("Filename", StringType(), True),
        StructField("Compressed", BooleanType(), True),
        StructField("Offset", StringType(), True),
        StructField("Gzip-Metadata", StructType([
            StructField("Deflate-Length", StringType(), True),
            StructField("Header-Length", StringType(), True),
            StructField("Footer-Length", StringType(), True),
            StructField("Inflated-CRC", StringType(), True),
            StructField("Inflated-Length", StringType(), True)
        ]), True)
    ]), True),
    StructField("Envelope", StructType([
        StructField("Payload-Metadata", StructType([
            StructField("Actual-Content-Length", StringType(), True),
            StructField("Trailing-Slop-Length", StringType(), True),
            StructField("Block-Digest", StringType(), True),
            StructField("Headers-Corrupt", BooleanType(), True),
            StructField("Actual-Content-Type", StringType(), True),
            StructField("WARC-Info-Metadata", StructType([
                StructField("isPartOf", StringType(), True),
                StructField("publisher", StringType(), True),
                StructField("description", StringType(), True),
                StructField("operator", StringType(), True),
                StructField("hostname", StringType(), True),
                StructField("software", StringType(), True),
                StructField("robots", StringType(), True),
                StructField("format", StringType(), True)
            ]), True)
        ]), True),
        StructField("Format", StringType(), True),
        StructField("WARC-Header-Length", StringType(), True),
        StructField("WARC-Header-Metadata", StructType([
            StructField("WARC-Type", StringType(), True),
            StructField("WARC-Date", StringType(), True),
            StructField("WARC-Record-ID", StringType(), True),
            StructField("Content-Length", StringType(), True),
            StructField("Content-Type", StringType(), True),
            StructField("WARC-Filename", StringType(), True)
        ]), True)
    ]), True)
])

# Parse the JSON strings into a DataFrame using the defined schema
df_flat = df_json.withColumn("parsed_value", from_json(col("value"), schema)).select("parsed_value.*")

# Flatten the nested structure further
df_flattened = df_flat.select(
    col("Container.Filename").alias("Container_Filename"),
    col("Container.Compressed").alias("Container_Compressed"),
    col("Container.Offset").alias("Container_Offset"),
    col("Container.Gzip-Metadata.Deflate-Length").alias("Container_Gzip_Metadata_Deflate_Length"),
    col("Container.Gzip-Metadata.Header-Length").alias("Container_Gzip_Metadata_Header_Length"),
    col("Container.Gzip-Metadata.Footer-Length").alias("Container_Gzip_Metadata_Footer_Length"),
    col("Container.Gzip-Metadata.Inflated-CRC").alias("Container_Gzip_Metadata_Inflated_CRC"),
    col("Container.Gzip-Metadata.Inflated-Length").alias("Container_Gzip_Metadata_Inflated_Length"),
    col("Envelope.Payload-Metadata.Actual-Content-Length").alias("Envelope_Payload_Metadata_Actual_Content_Length"),
    col("Envelope.Payload-Metadata.Trailing-Slop-Length").alias("Envelope_Payload_Metadata_Trailing_Slop_Length"),
    col("Envelope.Payload-Metadata.Block-Digest").alias("Envelope_Payload_Metadata_Block_Digest"),
    col("Envelope.Payload-Metadata.Headers-Corrupt").alias("Envelope_Payload_Metadata_Headers_Corrupt"),
    col("Envelope.Payload-Metadata.Actual-Content-Type").alias("Envelope_Payload_Metadata_Actual_Content_Type"),
    col("Envelope.Payload-Metadata.WARC-Info-Metadata.isPartOf").alias("Envelope_Payload_Metadata_WARC_Info_Metadata_isPartOf"),
    col("Envelope.Payload-Metadata.WARC-Info-Metadata.publisher").alias("Envelope_Payload_Metadata_WARC_Info_Metadata_publisher"),
    col("Envelope.Payload-Metadata.WARC-Info-Metadata.description").alias("Envelope_Payload_Metadata_WARC_Info_Metadata_description"),
    col("Envelope.Payload-Metadata.WARC-Info-Metadata.operator").alias("Envelope_Payload_Metadata_WARC_Info_Metadata_operator"),
    col("Envelope.Payload-Metadata.WARC-Info-Metadata.hostname").alias("Envelope_Payload_Metadata_WARC_Info_Metadata_hostname"),
    col("Envelope.Payload-Metadata.WARC-Info-Metadata.software").alias("Envelope_Payload_Metadata_WARC_Info_Metadata_software"),
    col("Envelope.Payload-Metadata.WARC-Info-Metadata.robots").alias("Envelope_Payload_Metadata_WARC_Info_Metadata_robots"),
    col("Envelope.Payload-Metadata.WARC-Info-Metadata.format").alias("Envelope_Payload_Metadata_WARC_Info_Metadata_format"),
    col("Envelope.Format").alias("Envelope_Format"),
    col("Envelope.WARC-Header-Length").alias("Envelope_WARC_Header_Length"),
    col("Envelope.WARC-Header-Metadata.WARC-Type").alias("Envelope_WARC_Header_Metadata_WARC_Type"),
    col("Envelope.WARC-Header-Metadata.WARC-Date").alias("Envelope_WARC_Header_Metadata_WARC_Date"),
    col("Envelope.WARC-Header-Metadata.WARC-Record-ID").alias("Envelope_WARC_Header_Metadata_WARC_Record_ID"),
    col("Envelope.WARC-Header-Metadata.Content-Length").alias("Envelope_WARC_Header_Metadata_Content_Length"),
    col("Envelope.WARC-Header-Metadata.Content-Type").alias("Envelope_WARC_Header_Metadata_Content_Type"),
    col("Envelope.WARC-Header-Metadata.WARC-Filename").alias("Envelope_WARC_Header_Metadata_WARC_Filename")
)

display(df_flattened)

total = df_flattened.count()
print(f"total: {total:,}")

# COMMAND ----------

display(df_json)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.2.1 Drop Files if needed 

# COMMAND ----------

# Drop all files from the directory
# for file in dbutils.fs.ls(f"s3://{destination_bucket}/common_crawl/wet_files/"):
    # dbutils.fs.rm(file.path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3. Text transformation

# COMMAND ----------

from pyspark.ml.feature import RegexTokenizer, StopWordsRemover
from pyspark.sql.functions import col

# Tokenize the text using RegexTokenizer
regex_tokenizer = RegexTokenizer(inputCol="value", outputCol="words", pattern="\\W")
df_tokenized = regex_tokenizer.transform(df_combined)

# Use StopWordsRemover to filter out non-English words
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
df_filtered = remover.transform(df_tokenized)

# Combine the filtered words back into a single string
df_combined_filtered = df_filtered.withColumn(
    "filtered_text", 
    concat_ws(" ", col("filtered_words"))
)

# Select the relevant columns
df_final = df_combined_filtered.select("filtered_text").distinct()

display(df_final)