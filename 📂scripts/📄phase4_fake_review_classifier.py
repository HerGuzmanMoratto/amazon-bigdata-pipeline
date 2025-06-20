# Fake Review Detection Analysis:
# Implementation: RandomForestClassifier

# step 1: Import necessary libraries
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF


# Initialize the session
spark = SparkSession.builder.appName("FakeReviewAnalysis").getOrCreate()

# Bring the dataset:
ds_ml = spark.read.option("header", True).option("inferSchema", True).csv("gs://assignment_pipeline/outputs/feature_engineered_ds/")

# Step 2: Select features and label
selected_cols2 = ["price", "helpful_vote", "verified_purchase", "text", "is_fake"]
ds_f = ds_ml.select(*selected_cols2).na.drop()

# Let's randomly sample 10% of the data (you can adjust this if needed)
ds_f_sampled = ds_f.sample(withReplacement=False, fraction=0.1, seed=42)

# Total columns after cleaning: 
# 3615845

# Step 3:  Text preprocessing
tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
remover = StopWordsRemover(inputCol="tokens", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="raw_features", numFeatures=1000)
idf = IDF(inputCol="raw_features", outputCol="text_features")

# Step 4: Assemble metadata features
meta_assembler = VectorAssembler(
    inputCols=["price", "helpful_vote", "verified_purchase"],
    outputCol="meta_features"
)

# Step 5: Combine text + meta_features 
final_assembler = VectorAssembler(
    inputCols=["meta_features", "text_features"],
    outputCol="features"
)


# Step 6: Build the ML pipeline
rf = RandomForestClassifier(labelCol="is_fake", featuresCol="features", numTrees=50)
pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, meta_assembler, final_assembler, rf])

# Step 7:  Train/test split and model fitting
train_data, test_data = ds_f_sampled.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train_data)
predictions = model.transform(test_data)


# Step 8. Evaluate the results:

binary_eval = BinaryClassificationEvaluator(labelCol="is_fake")
multi_eval = MulticlassClassificationEvaluator(labelCol="is_fake", predictionCol="prediction")

# Print the results:

print("Accuracy:", multi_eval.setMetricName("accuracy").evaluate(predictions))
print("F1 Score:", multi_eval.setMetricName("f1").evaluate(predictions))
print("AUC:", binary_eval.setMetricName("areaUnderROC").evaluate(predictions))
print("PR AUC:", binary_eval.setMetricName("areaUnderPR").evaluate(predictions))

# Results:

Accuracy: 0.9628506968954588                                                    
F1 Score: 0.9446275928967887                                                    
AUC: 0.9940501158840711                                                         
PR AUC: 0.7454572107981 

# Step 9: Evaluate all metrics and store in a dictionary
results_fake_review = {
    "Accuracy": multi_eval.setMetricName("accuracy").evaluate(predictions),
    "F1 Score": multi_eval.setMetricName("f1").evaluate(predictions),
    "AUC": binary_eval.setMetricName("areaUnderROC").evaluate(predictions),
    "PR AUC": binary_eval.setMetricName("areaUnderPR").evaluate(predictions)
}


# Step 10: Save results to a local file
local_path = "/tmp/fake_review_results.txt"

with open(local_path, "w") as f:
    f.write("=== Fake Review Detection Results ===\n")
    for metric, value in results_fake_review.items():
        f.write(f"{metric}: {value:.4f}\n")

print("Results saved locally at:", local_path)

# 10.1 upload the txt results file to the bucket:
import subprocess

# Upload to GCS
gcs_path = "gs://assignment_pipeline/outputs/fake_review_results.txt"

subprocess.run(["gsutil", "cp", local_path, gcs_path], check=True)
print("File uploaded to GCS successfully:", gcs_path)



# Step 11: Save the model to a  temporary directory

local_model_path = "/tmp/fake_review_model"

# Save the trained pipeline model
model.write().overwrite().save(local_model_path)
print("Model saved locally at:", local_model_path)

 # Step 12:  Upload the model directory to GCS

from pyspark.ml.pipeline import PipelineModel
import subprocess

# Save the trained pipeline model to local 
try:
    pipeline_model.write().overwrite().save("/tmp/fake_review_model")
    print("Model saved locally at: /tmp/fake_review_model")
except Exception as e:
    print("Local save failed or path not accessible:", str(e))

# Save the trained pipeline model directly to GCS 
gcs_model_path = "gs://assignment_pipeline/outputs/models/fake_review_model"
pipeline_model.write().overwrite().save(gcs_model_path)
print("Model uploaded to GCS at:", gcs_model_path)

