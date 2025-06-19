########### PHASE ONE: GCloud Initialization & Local directory copy  ############

### cluster connection, and copy files into local session for handling.
## First we need to initialize the cluster environment:
## step 1: Enter to the G-Cloud environment:
gcloud compute ssh bda-cluster-m --project= trusty-sentinel-455807-j6 --zone=europe-west2-c

# Step 2: Copy the raw file into a local directory:
#dataset 1:
gsutil cp gs://assignment_pipeline/Industrial_and_Scientific.jsonl .
#dataset 2:
gsutil cp gs://assignment_pipeline/meta_Industrial_and_Scientific.jsonl .

# Steo 3: confirm that files are there:
#file 1:
ls -lh Industrial_and_Scientific.jsonl
#file 2:
ls -lh meta_Industrial_and_Scientific.jsonl

# Step 4: Create a file directory data/reviews:
hdfs dfs -mkdir -p /data/reviews

# Step 5: Move the file into the folder:
#file 1:
hdfs dfs -put Industrial_and_Scientific.jsonl /data/reviews/
#file 2:
hdfs dfs -put meta_Industrial_and_Scientific.jsonl /data/reviews/
