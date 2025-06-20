#### 📄phase3_feature_engineering 
# Importing libraries, the dataset and Initializating session to the dataset:
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np


spark = SparkSession.builder.appName("DataEngineeringPhase").getOrCreate()
# Load cleaned dataset
ds_clean = spark.read.option("header", True).option("inferSchema", True).csv("gs://assignment_pipeline/Industrial_and_Scientific_clean/")

## Check structure to follow best practices:
ds_clean.printSchema()

# Step 1: we filter out invalid values identified with EDA:
cleaned = ds_clean.filter(
    (col("main_category").isNotNull()) &
    (col("main_category") != "") &
    (~col("main_category").like("true")) &
    (col("rating").isNotNull()) &
    (col("helpful_vote").isNotNull()) &
    (col("price").isNotNull()) &
    (col("price").between(1, 10000))
)


## Step 2: Cast relevant columns and create new features:
ds_labeled = cleaned \
    .withColumn("helpful_vote", col("helpful_vote").cast("int")) \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("rating", col("rating").cast("double")) \
    .withColumn("verified_purchase", col("verified_purchase").cast("int")) \
    .withColumn("sentiment_label", when(col("rating") >= 4, 1).when(col("rating") <= 2, 0)) \
    .filter(col("rating").isNotNull() & col("sentiment_label").isNotNull()) \
    .withColumn("is_fake", when((col("verified_purchase") == 0) & (col("helpful_vote") == 0), 1).otherwise(0))

# Step 2: Select numeric columns
numeric_cols = ["price", "helpful_vote", "verified_purchase", "sentiment_label", "is_fake", "rating", "average_rating", "rating_number"]

# then we create a numeric dataset:
ds_numeric = ds_labeled.select([col(c).cast("double") for c in numeric_cols])
# check for nule values:
from pyspark.sql.functions import col, when, sum as spark_sum

null_counts = ds_labeled.select([
    spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(f"{c}_nulls")
    for c in numeric_cols
])

# Show the result
null_counts.show()

# we drop null values is price:
## ds_labeled = ds_labeled.filter(col("price").isNotNull())


# Step 3: Assemble features into vector column
vec_assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features_vector")
ds_vector = vec_assembler.transform(ds_labeled).select("features_vector")

# Step 4: Compute Pearson correlation matrix
corr_matrix = Correlation.corr(ds_vector, "features_vector", method="pearson").head()[0]

# Step 5: Convert to numpy array for readable format.
corr_array = corr_matrix.toArray()


# Step 5.1: Create labeled correlation matrix using pandas
feature_names = numeric_cols  
corr_df = pd.DataFrame(corr_array, index=feature_names, columns=feature_names)
print(corr_df)
## Results:

                      price  helpful_vote  verified_purchase  ...    rating  average_rating  rating_number
price              1.000000      0.024932          -0.050321  ... -0.010902       -0.084184      -0.038756
helpful_vote       0.024932      1.000000          -0.001991  ... -0.026870       -0.011779       0.002589
verified_purchase -0.050321     -0.001991           1.000000  ...  0.010035        0.029570       0.034520
sentiment_label   -0.008715     -0.024699           0.003818  ...  0.964516        0.245807      -0.054227
is_fake            0.019638     -0.017248          -0.876106  ...  0.011763       -0.010517      -0.027876
rating            -0.010902     -0.026870           0.010035  ...  1.000000        0.258504      -0.052696
average_rating    -0.084184     -0.011779           0.029570  ...  0.258504        1.000000       0.041619
rating_number     -0.038756      0.002589           0.034520  ... -0.052696        0.041619       1.000000


## step 6. Save correlation matrix to file (as plain text)
np.savetxt("/tmp/correlation_matrix.txt", corr_array, delimiter=",", fmt="%.2f")


local_path = "/tmp/correlation_matrix.txt"
with open(local_path, "w") as f:
    f.write("Pearson Correlation Matrix\n")
    f.write(corr_df.to_string())


# Step 7. Run subprocess to save the file:

import subprocess

subprocess.run([
    "gsutil", "cp",
    "/tmp/correlation_matrix.txt",
    "gs://assignment_pipeline/outputs/correlation_matrix2.txt"
], check=True)



## Step 8.
# Select  final columns
final_cols = [
    "asin",                 # Product ID (for grouping/analysis)
    "price",                # Numeric feature
    "helpful_vote",         # Numeric feature
    "verified_purchase",    # Categorical/binary feature
    "rating",               # user rating 
    "sentiment_label",      # Main classification label (positive vs negative)
    "is_fake",              # 2nd label (fake review detection)
    "average_rating",       # Metadata feature
    "rating_number",        # number of ratings
    "main_category",        # Useful for segmentation
    "text"                  # Raw text 
]

# Step 9. 
#Create the final dataset:
final_ds = ds_labeled.select(final_cols)

#Step 10: 
# Save final dataset to GCS in a single file
final_ds.coalesce(1).write \
    .option("header", True) \
    .mode("overwrite") \
    .csv("gs://assignment_pipeline/feature_engineered_ds/")


Final dataset records:
first cleaning process with errors:
3653916                            
Second cleaning process:
3620007                                              

dataset for fake review:
3615845