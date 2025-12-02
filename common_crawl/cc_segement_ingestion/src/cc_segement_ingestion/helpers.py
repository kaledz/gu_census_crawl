import boto3
import pandas as pd
import json
from functools import reduce
from pyspark.sql import functions as F

class cc():
    def list_master_indexes():
        # List all master indexes from the Common Crawl S3 bucket
        bucket = "commoncrawl"
        prefix = "crawl-data/"
        paginator = s3.get_paginator("list_objects_v2")
        result = []
        for page in paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter="/"
        ):
            result.extend(page.get("CommonPrefixes", []))
        return pd.DataFrame([p['Prefix'] for p in result], columns=["master_index"])
    
    def list_crawls(prefix: str, as_json: bool = False, s3_client=None) -> pd.DataFrame | str:
        """
        List S3 objects under the given Common Crawl prefix and return as pandas DataFrame (or JSON).
        Example prefix: 'crawl-data/CC-MAIN-2025-05/'  (trailing slash recommended)
        """
        bucket_name = "commoncrawl"
        s3c = s3_client or s3

        # normalize prefix
        if not prefix or not isinstance(prefix, str):
            raise ValueError(f"prefix must be a non-empty str, got: {type(prefix).__name__}={prefix!r}")
        if not prefix.endswith("/"):
            prefix = prefix + "/"

        object_metadata = []
        paginator = s3c.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            # some pages may not have 'Contents' (e.g., empty prefixes)
            contents = page.get("Contents", [])
            if contents:
                object_metadata.extend(contents)

        if not object_metadata:
            # empty result
            if as_json:
                return json.dumps([])
            return pd.DataFrame()

        df = pd.DataFrame(object_metadata)
        if as_json:
            return df.to_json(orient="records")
        return df
        
    def batch_crawl_list(df, column):
        # Prepare a list of crawl prefixes from the given DataFrame column
        crawl_list = (
            df[column]
            .astype(str)
            .str.strip()
            .apply(lambda s: s if s.endswith("/") else s + "/")
            .tolist()
        )

        # de-dupe while preserving order
        seen = set()
        crawl_list = [p for p in crawl_list if p and not (p in seen or seen.add(p))]
        return crawl_list
    
    def batch_ingest_crawls(crawl_list):
        """
        Runs list_crawls(prefix) for each prefix and unions into a single Spark DataFrame.
        Assumes list_crawls returns a pandas DataFrame.
        """
        spark_parts = []
        total = len(crawl_list)

        for i, pfx in enumerate(crawl_list, 1):
            print(f"[{i}/{total}] Fetching: {pfx}")
            pdf = cc.list_crawls(pfx, as_json=False)  # <-- just call directly
            if pdf is None or pdf.empty:
                print(f"  -> empty; skipping {pfx}")
                continue

            sdf = spark.createDataFrame(pdf).withColumn("crawl_prefix", F.lit(pfx))
            spark_parts.append(sdf)

        if not spark_parts:
            raise RuntimeError("No data returned to union.")

        # Union all Spark DataFrames into a single DataFrame
        df_crawls = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), spark_parts)
        print(f"Total rows: {df_crawls.count()} | Prefixes combined: {len(spark_parts)}")
        return df_crawls