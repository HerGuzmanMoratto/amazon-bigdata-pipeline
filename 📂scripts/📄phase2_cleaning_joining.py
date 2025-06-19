################### Phase 2 Dataset Cleaning & merging ###################


## Script for Pre processing steps using PySpark:

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("IndustrialReview").getOrCreate()

# Step 1: Define the path to create the dataframe:
#We are using two datasets to later join:
#Dataset 1
df = spark.read.json("hdfs:///data/reviews/Industrial_and_Scientific.jsonl")

#Dataset 2: meta data. This file has some nested columns previsualized in Jupiter notebook.

#For this, we first bring the raw data as text
raw_meta = spark.read.text("hdfs:///data/reviews/meta_Industrial_and_Scientific.jsonl")

#then we import necessary libraries:
from pyspark.sql.functions import from_json, col, regexp_replace
from pyspark.sql.types import *

#now we can define the schema based on the columns preview we got on Jupiter notebook:
schema_meta = StructType([
    StructField("main_category", StringType(), True),
    StructField("title", StringType(), True),
    StructField("average_rating", DoubleType(), True),
    StructField("rating_number", IntegerType(), True),
    StructField("features", StringType(), True),    	
    StructField("description", StringType(), True),
    StructField("price",  StringType(), True),
    StructField("images", StringType(), True),
    StructField("videos", StringType(), True),
    StructField("store", StringType(), True),
    StructField("categories", StringType(), True),
    StructField("details", StringType(), True),
    StructField("parent_asin", StringType(), True),
    StructField("bought_together", StringType(), True)
])
# Convert and extract files from JSON
metadata = raw_meta.select(from_json(col("value"), schema_meta).alias("data")).select("data.*")

# Then we create the clean meta dataset:
df2 = metadata.withColumn("price", regexp_replace("price", "[$,]", "").cast("float"))


# Step 2: check the dataset schema:
#Schema dataset 1
df.printSchema()

#Schema dataset 2:
df2.printSchema()

## The first schema showed the following structure:
##	root
##		 |-- asin: string (nullable = true)
##		 |-- helpful_vote: long (nullable = true)
##		 |-- images: array (nullable = true)
##		 |    |-- element: struct (containsNull = true)
##		 |    |    |-- attachment_type: string (nullable = true)
##		 |    |    |-- large_image_url: string (nullable = true)
##		 |    |    |-- medium_image_url: string (nullable = true)
##		 |    |    |-- small_image_url: string (nullable = true)
##		 |-- parent_asin: string (nullable = true)
##		 |-- rating: double (nullable = true)
##		 |-- text: string (nullable = true)
##		 |-- timestamp: long (nullable = true)
##		 |-- title: string (nullable = true)
##		 |-- user_id: string (nullable = true)
##		 |-- verified_purchase: boolean (nullable = true)

## second schema structure:
##		root
##		 |-- main_category: string (nullable = true)
##		 |-- title: string (nullable = true)
##		 |-- average_rating: double (nullable = true)
##		 |-- rating_number: integer (nullable = true)
##		 |-- features: string (nullable = true)
##		 |-- description: string (nullable = true)
##		 |-- price: double (nullable = true)
##		 |-- images: string (nullable = true)
##		 |-- videos: string (nullable = true)
##		 |-- store: string (nullable = true)
##		 |-- categories: string (nullable = true)
##		 |-- details: string (nullable = true)
##		 |-- parent_asin: string (nullable = true)
##		 |-- bought_together: string (nullable = true)

# As there are two columns with the same name, we drop them from the meta file:
# drop duplicates from metadata
df2_clean = df2.drop("title", "images")  

# additionally, we drop images column from the main file, as the scope of this project does not include image analysis.
df= df.drop("images")

# we join the two datasets to have a clean dataset with a left join:
ds_clean = df.join(df2_clean, on="parent_asin", how="left")


# now we check the schema:
ds_clean.printSchema()
## Result:

##		root
##		 |-- parent_asin: string (nullable = true)
##		 |-- asin: string (nullable = true)
##		 |-- helpful_vote: long (nullable = true)
##		 |-- rating: double (nullable = true)
##		 |-- text: string (nullable = true)
##		 |-- timestamp: long (nullable = true)
##		 |-- title: string (nullable = true)
##		 |-- user_id: string (nullable = true)
##		 |-- verified_purchase: boolean (nullable = true)
##		 |-- main_category: string (nullable = true)
##		 |-- average_rating: double (nullable = true)
##		 |-- rating_number: integer (nullable = true)
##		 |-- features: string (nullable = true)
##		 |-- description: string (nullable = true)
##		 |-- price: double (nullable = true)
##		 |-- videos: string (nullable = true)
##		 |-- store: string (nullable = true)
##		 |-- categories: string (nullable = true)
##		 |-- details: string (nullable = true)
##		 |-- bought_together: string (nullable = true)

## now we save the clean dataset as csv in our bucket, so we can use it at any time without further handling:
# We use coalesce to ensure just one file as output.
ds_clean.coalesce(1).write.option("header", "true").option("delimiter", ",").mode("overwrite").csv("gs://assignment_pipeline/Industrial_and_Scientific_clean")


# We ensure the file is stored in the bucket:
gsutil ls gs://assignment_pipeline/Industrial_and_Scientific_clean/

### First part, file preparation is now completed.
