# Phase 2.2 REPLICATE HIVE EDA WITH PYSPARK FOR PERFORMANCE PURPOSES.
# Step 1: Load the Clean CSV from GCS 
# Importing the dataset and Initializating session to the dataset:
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("SparkIndustrialEDA").getOrCreate()

# Load the CSV file
ds_clean = spark.read.option("header", True).option("inferSchema", True).csv("gs://assignment_pipeline/Industrial_and_Scientific_clean/")
Import time
start = time.time()
ds_clean.printSchema()
end = time.time()
print(f"Time taken: {round(end - start, 2)} seconds")

# Replicate initial exploration as was done with Hive:
# Step 2: Replicate total review count.

#Count total rows
start = time.time()
ds_clean.count()
end = time.time()
print(f"Time taken: {round(end - start, 2)} seconds")
# Output
# 5183005
                                                                      
# Time taken: 57.92 seconds
# Step 3: Verified purchases
start = time.time()
ds_clean.filter(ds_clean["verified_purchase"] == True).count()
end = time.time()
print(f"Time taken: {round(end - start, 2)} seconds")

# Output:
# 4909835                                                                         
# Time taken: 72.26 seconds

# Step 4: Average rating
from pyspark.sql.functions import avg
start = time.time()
ds_clean.select(avg("rating")).show()
end = time.time()
print(f"Time taken: {round(end - start, 2)} seconds")

# Output:
# +-----------------+                                                             
# |      avg(rating)|
# +-----------------+
# |4.183227297677699|
# +-----------------+
# Time taken: 72.53 seconds

# Step 5:  Max and Min rating
from pyspark.sql.functions import max as max_, min as min_
start = time.time()
ds_clean.select(max_("rating"), min_("rating")).show()
end = time.time()
print(f"Time taken: {round(end - start, 2)} seconds")

# Output:
# +-----------+-----------+                                                       
# |max(rating)|min(rating)|
# +-----------+-----------+
# |        5.0|        1.0|
# +-----------+-----------+
# Time taken: 71.51 seconds

 # Step 6: Top 10 reviewed products 
start = time.time()
ds_clean.groupBy("parent_asin").count().orderBy("count", ascending=False).show(10)
end = time.time()
print(f"Time taken: {round(end - start, 2)} seconds")

# Output:
# +-----------+-----+                                                             
# |parent_asin|count|
# +-----------+-----+
# | B0B6D1KQNJ|18097|
# | B09KZ6TBNY|15842|
# | B08WY4PXLT|11291|
# | B0B6YT7HNF|11055|
# | B07GGZT8S7| 9823|
# | B0BGXZLXNJ| 9792|
# | B0BGQR74H1| 8943|
# | B09WW4Z413| 8387|
# | B007Q2M17K| 8126|
# | B098BR67DJ| 7597|
# +-----------+-----+
# only showing top 10 rows
# Time taken: 85.32 seconds

# Step 7: Helpful votes range
from pyspark.sql.functions import max as max_, min as min_
start = time.time()
ds_clean.select(max_("helpful_vote"), min_("helpful_vote")).show()
end = time.time()
print(f"Time taken: {round(end - start, 2)} seconds")
# Output: 
# +-----------------+-----------------+                                           
# |max(helpful_vote)|min(helpful_vote)|
# +-----------------+-----------------+
# |             5611|               -1|
# +-----------------+-----------------+
# Time taken: 69.13 seconds

# Step 8: Helpfulness vote distribution:
from pyspark.sql.functions import when, col
start = time.time()
vote_distribution = ds_clean.withColumn(
    "vote_range",
    when(col("helpful_vote") < 0, "-1")
    .when(col("helpful_vote") == 0, "0")
    .when((col("helpful_vote") >= 1) & (col("helpful_vote") <= 10), "1–10")
    .when((col("helpful_vote") >= 11) & (col("helpful_vote") <= 100), "11–100")
    .when((col("helpful_vote") >= 101) & (col("helpful_vote") <= 1000), "101–1,000")
    .otherwise("1,001+")
)

vote_distribution.groupBy("vote_range").count().orderBy("count", ascending=False).show()
end = time.time()
print(f"Time taken: {round(end - start, 2)} seconds")

# Output:
# +----------+-------+                                                            
# |vote_range|  count|
# +----------+-------+
# |         0|3991975|
# |      1–10|1120393|
# |    11–100|  67083|
# | 101–1,000|   3478|
# |    1,001+|     75|
# |        -1|      1|
# +----------+-------+
# Time taken: 78.0 seconds

# Step 9: Raw summary (with outliers)
start = time.time()

ds_clean.groupBy("main_category") \
    .agg(
        count("*").alias("total_reviews"),
        avg("rating").alias("avg_rating"),
        max_("helpful_vote").alias("max_votes"),
        avg("price").alias("avg_price")
    ).orderBy("avg_rating", ascending=False).show()
end = time.time()
print(f"Time taken: {round(end - start, 2)} seconds")
# Output:
# +--------------------+-------------+------------------+---------+------------------+
# |       main_category|total_reviews|        avg_rating|max_votes|         avg_price|
# +--------------------+-------------+------------------+---------+------------------+
# |   Collectible Coins|            4|               5.0|        1|              null|
# |      Amazon Devices|            2|               5.0|        1|              null|
# |         Movies & TV|          299|  4.65886287625418|      310|12.204104046242785|
# |       Digital Music|          373| 4.536193029490617|       41|  12.6951234567901|
# |      Premium Beauty|           26|               4.5|        2| 33.59777777777778|
# |          Appliances|        19172| 4.470164823701231|      201|103.48687862032656|
# |     Car Electronics|         7800| 4.441282051282052|      107| 19.45771161892509|
# |               Books|         1361| 4.402645113886848|       83|  21.4414639397201|
# | Musical Instruments|         4313|4.3243681891954555|      340| 65.80024295432445|
# |   Sports & Outdoors|        38495| 4.306819067411352|      353| 32.11365527291989|
# |            Handmade|           28| 4.285714285714286|        7|17.214814814814815|
# |Cell Phones & Acc...|        11252|4.2789726270885176|      236| 21.31623982727136|
# |                Baby|         1606|4.2727272727272725|      132|23.344957659738228|
# |             Grocery|         6725| 4.260966542750929|      645|16.204238026124745|
# |          Automotive|       125589| 4.254464961103281|     1571|37.930960853758386|
# |           Computers|        16836| 4.235982418626752|      380| 40.69567801237716|
# |Home Audio & Theater|         9876| 4.225597407857432|      193| 32.46169520919139|
# |Arts, Crafts & Se...|        22484| 4.220601316491727|      262|23.435248868778217|
# |        Toys & Games|        17108| 4.214168809913491|      192|27.375659654272628|
# |Tools & Home Impr...|      1070497| 4.209531647449736|     2503|32.222265882714204|
# +--------------------+-------------+------------------+---------+------------------+
# only showing top 20 rows
# Time taken: 89.91 seconds

# Step 10: Cleaned summary (filtering garbage)
start = time.time()
cleaned = ds_clean.filter(
    (col("main_category").isNotNull()) &
    (col("main_category") != "") &
    (~col("main_category").like("true")) &
    (col("rating").isNotNull()) &
    (col("helpful_vote").isNotNull()) &
    (col("price").isNotNull()) &
    (col("price").between(1, 10000))
)

cleaned.groupBy("main_category").agg(
        count("*").alias("total_valid_reviews"),
        avg("rating").alias("avg_rating"),
        max_("helpful_vote").alias("max_votes"),
        avg("price").alias("avg_price")
    ).filter("total_valid_reviews > 500").orderBy("total_valid_reviews", ascending=False).show()
end = time.time()
print(f"Time taken: {round(end - start, 2)} seconds")
# Output:

# +--------------------+-------------------+------------------+---------+------------------+
# |       main_category|total_valid_reviews|        avg_rating|max_votes|         avg_price|
# +--------------------+-------------------+------------------+---------+------------------+
# |Industrial & Scie...|            1897307| 4.220128318717003|     5611| 45.00780555281352|
# |Tools & Home Impr...|             812085| 4.234386794485799|     2503| 32.03871116939639|
# |         Amazon Home|             453134| 4.248482788755644|     2846|58.599854855296016|
# |Health & Personal...|             192209|  4.11875614565395|     1708|23.846424725168642|
# |     Office Products|             109519| 4.187136478601887|     1213| 26.68894557108829|
# |          Automotive|             106442| 4.272176396535202|     1571|37.790064636140045|
# |      AMAZON FASHION|              45421| 4.081085841350917|     1131|18.304753968428983|
# |          All Beauty|              42522|3.9433704905695874|     1524|16.097293401062966|
# |     All Electronics|              37309| 4.089978289420783|      574|187.29537055402093|
# |   Sports & Outdoors|              30189| 4.317234754380735|      353|  32.1167481533008|
# |        Pet Supplies|              25686|3.9089776531962936|     1573|   23.510111344701|
# |Arts, Crafts & Se...|              16133| 4.229901444244716|      262|23.435248868778196|
# |          Appliances|              15191| 4.485813968797314|      201|102.83485748140363|
# |        Toys & Games|              12263| 4.260539835276849|      192|27.377811302291416|
# |      Camera & Photo|              12089| 4.142608983373314|      698| 80.53367276036063|
# |           Computers|              10987|   4.2880677163921|      380|40.699315554746526|
# |     Car Electronics|               6214| 4.449308014161571|      107| 19.45771161892509|
# |Cell Phones & Acc...|               6021|4.3809998339146325|      177| 21.31623982727136|
# |             Grocery|               5512|   4.2322206095791|      645|16.204238026124745|
# |Home Audio & Theater|               4613| 4.347062649035335|      179|32.461695209191376|
# +--------------------+-------------------+------------------+---------+------------------+
# only showing top 20 rows
# Time taken: 82.95 seconds

# Step 11: Price Bucket Distribution

start = time.time()
price_buckets = cleaned.withColumn(
    "price_range",
    when(col("price") < 10, "Under $10")
    .when((col("price") >= 10) & (col("price") < 50), "$10-50")
    .when((col("price") >= 50) & (col("price") < 100), "$50-100")
    .when((col("price") >= 100) & (col("price") < 500), "$100-500")
    .when(col("price") >= 500, "Over $500")
    .otherwise("Unknown")
)

price_buckets.groupBy("price_range") \
    .agg(
        count("*").alias("total_products"),
        avg("rating").alias("avg_rating")
    ).orderBy("total_products", ascending=False).show()
end = time.time()
print(f"Time taken: {round(end - start, 2)} seconds")

# Output:
# +-----------+--------------+-----------------+                                  
# |price_range|total_products|       avg_rating|
# +-----------+--------------+-----------------+
# |     $10-50|       2327084|4.229048457210827|
# |  Under $10|        884650|4.194060928050641|
# |    $50-100|        326099|4.220374794157602|
# |   $100-500|        282860|4.199430813830164|
# |  Over $500|         26183|4.034220677538861|
# +-----------+--------------+-----------------+
# Time taken: 78.76 seconds



