# Sentiment Classification Analysis:

# Implementation: LogisticRegression, DecisionTreeClassifier, RandomForestClassifier

# step 1: Import necessary libraries
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession

# Initialize the session

spark = SparkSession.builder.appName("ClasificationAnalysis").getOrCreate()

# Bring the dataset:
ds_ml = spark.read.option("header", True).option("inferSchema", True).csv("gs://assignment_pipeline/outputs/feature_engineered_ds/")


# Step 2: Select features and label
selected_features = ["price", "helpful_vote", "verified_purchase"]
label_col = "sentiment_label"

# step 4: Prepare dataset for ML
# use assembler:

assembler = VectorAssembler(
    inputCols=selected_features,
    outputCol="features"
)

# Create data_ml dataset:
data_ml = ds_ml.select(*(selected_features + [label_col]))
data_ml = assembler.transform(data_ml).select("features", label_col)

# Step 5: split dataset for training and test purposes.

train_data, test_data = data_ml.randomSplit([0.8, 0.2], seed=42)

# Step 6: Define the models to implement:

lr = LogisticRegression(labelCol=label_col, featuresCol="features")
dt = DecisionTreeClassifier(labelCol=label_col, featuresCol="features")
rf = RandomForestClassifier(labelCol=label_col, featuresCol="features", numTrees=50)

# Step 7: Train the models:

lr_model = lr.fit(train_data)
dt_model = dt.fit(train_data)
rf_model = rf.fit(train_data)

# Step 8: Make predictions based on test data:
# 8. Make predictions
lr_preds = lr_model.transform(test_data)
dt_preds = dt_model.transform(test_data)
rf_preds = rf_model.transform(test_data)

# Step 9: Define the evaluation metrics:
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
def evaluate_binary_model(predictions, label_col):
    multi_eval = MulticlassClassificationEvaluator(labelCol=label_col, predictionCol="prediction")
    binary_eval = BinaryClassificationEvaluator(labelCol=label_col, rawPredictionCol="rawPrediction")
    results = {}
    results["Accuracy"] = multi_eval.setMetricName("accuracy").evaluate(predictions)
    results["F1 Score"] = multi_eval.setMetricName("f1").evaluate(predictions)
    results["Weighted Precision"] = multi_eval.setMetricName("weightedPrecision").evaluate(predictions)
    results["Weighted Recall"] = multi_eval.setMetricName("weightedRecall").evaluate(predictions)
    results["AUC"] = binary_eval.setMetricName("areaUnderROC").evaluate(predictions)
    results["PR AUC"] = binary_eval.setMetricName("areaUnderPR").evaluate(predictions)
    return results

# Step 10: Start subprocess to check and save the metric resuls:
import subprocess
# Create a dictionary to hold all evaluation results
all_results = {
    "Logistic Regression": evaluate_binary_model(lr_preds, label_col),
    "Decision Tree": evaluate_binary_model(dt_preds, label_col),
    "Random Forest": evaluate_binary_model(rf_preds, label_col)
}

# Step 10.Print results
for model_name, metrics in all_results.items():
    print(f"\n=== {model_name} ===")
    for metric, value in metrics.items():
        print(f"{metric}: {value:.4f}")


# Results:
=== Logistic Regression ===
Accuracy: 0.8396
F1 Score: 0.7666
Weighted Precision: 0.7427
Weighted Recall: 0.8396
AUC: 0.5589
PR AUC: 0.8583

=== Decision Tree ===
Accuracy: 0.8397
F1 Score: 0.7665
Weighted Precision: 0.7051
Weighted Recall: 0.8397
AUC: 0.5000
PR AUC: 0.8397

=== Random Forest ===
Accuracy: 0.8397
F1 Score: 0.7665
Weighted Precision: 0.7051
Weighted Recall: 0.8397
AUC: 0.5893
PR AUC: 0.8678


# Step 11: Save results:

# Create the results path:
results_path = "/tmp/classification_results.txt"

#Then  save the results, we use a function so the results can be readable:

def save_results_to_file(results_dict, local_path):
    with open(local_path, "w") as f:
        for model_name, metrics in results_dict.items():
            f.write(f"\n=== {model_name} ===\n")
            for metric, value in metrics.items():
                f.write(f"{metric}: {value:.4f}\n")

# Call the function to write the results
results_path = "/tmp/classification_results.txt"
save_results_to_file(all_results, results_path)

# Store the results in the bucket:
import subprocess
subprocess.run(["gsutil", "cp",
    "/tmp/classification_results.txt",
    "gs://assignment_pipeline/outputs/classification_results.txt"
], check=True)
print(" File uploaded to GCS successfully.")


