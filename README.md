# amazon-bigdata-pipeline

# Detecting Fake Reviews and Sentiment Classification in Amazon’s Industrial & Scientific Category: An End-to-End Big Data Pipeline Approach

This project demonstrates how to build a scalable big data pipeline using Hadoop, Spark, Hive, and PySpark MLlib to analyze customer reviews from the **Industrial & Scientific** category of the Amazon Reviews dataset. It focuses on two key machine learning tasks: **fake review detection** and **sentiment classification**.

📌 Project Overview
In the digital commerce era, user-generated content plays a crucial role in consumer behavior and business models. The research is focused on developing a scalable Big Data pipeline for Amazon review processing and analysis within the Industrial & Scientific category.
The primary objectives are sentiment classification to determine the tone of reviews and fake review detection based on metadata and textual features. These efforts aim to enhance user trust, improve recommendation systems, and automate content moderation.
Following the CRISP-DM methodology, the approach employs a big data stack, including Google Cloud Platform, HDFS, Apache Hive, and PySpark. Data ingestion, cleaning, feature engineering, and model development were performed in a distributed environment.
Correlation analysis helped to identify relevant features to implement in the classification models, Random Forest Classifier, Logistic Regression, and Decision Tree. The results indicate that combining structured metadata with textual data improves model performance, with the fake review detection model achieving over 94% accuracy. This project showcases modern big data tools and offers a replicable framework for e-commerce analytics


## 📌 Objectives

- Develop an end-to-end big data pipeline using modern distributed technologies.
- Clean and preprocess large-scale JSONL review and metadata files.
- Perform exploratory data analysis using Hive and PySpark.
- Build supervised learning models to classify:
  - Whether a review is **fake** or **authentic**.
  - Whether the review sentiment is **positive** or **negative**.
- Compare model performance using metrics such as Accuracy, F1, AUC, and PR AUC.

## 🛠️  Technologies Used

- **Google Cloud Platform (GCP)** – Infrastructure, Dataproc cluster, and Cloud Storage
- **Apache Hadoop (HDFS)** – Distributed file system
- **Apache Hive** – SQL-like EDA and query analysis
- **Apache Spark / PySpark** – Data cleaning, transformation, and modeling
- **MLlib (Spark)** – Machine learning implementation:
- Implement classification models for:
    -Fake review detection
    -Sentiment classification
- **Python / Jupyter Notebook** – Local data validation
- **GitHub** – Code versioning and outputs

  ## 📂 Datasets

This project uses two files from the Amazon Customer Reviews Dataset:
- `Industrial_and_Scientific.jsonl` – Review content including ratings, votes, and text.
- `Industrial_and_Scientific_meta.jsonl` – Product metadata including price, category, and product details.

> Source: [Amazon Customer Reviews Dataset]([https://s3.amazonaws.com/amazon-reviews-pds/readme.html](https://amazon-reviews-2023.github.io/))

## 🤖 Machine Learning Tasks

### 1. Fake Review Detection
- **Label (`is_fake`)**:
  - 1 = Potentially fake (no helpful votes + not verified)
  - 0 = Authentic
- **Features**:
  - Structured metadata: `price`, `helpful_vote`, `verified_purchase`
  - Text features: TF-IDF from review `text`

### 2. Sentiment Classification
- **Label (`sentiment_label`)**:
  - 1 = Positive (ratings 4-5)
  - 0 = Negative (ratings 1-2)
- **Features**:
  - Structured metadata: `price`, `helpful_vote`, `verified_purchase`

##  📈 Model Evaluation Metrics

- Accuracy
- F1 Score
- AUC (Area Under ROC)
- PR AUC (Precision-Recall Area)


## 📤Outputs

- Fake_review_results.txt
- Classification_results.txt
- Correlation_matrix.txt

## Folder Structure

├── models/
│   ├── fake_review_model/
│   ├── fake_review_model2/
│   ├── logistic_regression_model/
│   └── random_forest_model/      
├── scripts/                   # PySpark and Hive scripts    # Jupyter Notebooks for prototyping
└── README.md

## 📚 Methodology
- This project follows the CRISP-DM methodology:

   - Business Understanding

   - Data Understanding

   - Data Preparation

   - Modeling

   - Evaluation

  - Reproducibility setup in GCS

## *** IMPORTANT NOTE: THIS PROJECT WAS DEVELOPED FOR EDUCATIONAL PURPOSES, USE IT AS A GUIDE OR BASIS FOR FURTHER OR DEEPER ANALYSIS IN THE STUDIED AREAS. ***
## ** While results are realistic, further validation is needed for production-level deployment. **

📬 Contact
- Hernan Guzman
- Industrial engineer, Data Analyst & Big Data Enthusiast
- Email: herguzman12@gmail.com
- LinkedIn: www.linkedin.com/in/hernan-guzman-m-741124207
