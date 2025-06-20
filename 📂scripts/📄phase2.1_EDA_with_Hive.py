Script Phase 2.1
## Phase 2.1 EDA and Summary Statistics with Hive:

# Step 1: Create the table in the hive environment.
CREATE EXTERNAL TABLE IF NOT EXISTS industrial_reviews (
  parent_asin STRING,
  asin STRING,
  helpful_vote BIGINT,
  rating DOUBLE,
  text STRING,
  `timestamp` BIGINT,
  title STRING,
  user_id STRING,
  verified_purchase BOOLEAN,
  main_category STRING,
  average_rating DOUBLE,
  rating_number INT,
  features STRING,
  description STRING,
  price DOUBLE,
  videos STRING,
  store STRING,
  categories STRING,
  details STRING,
  bought_together STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://assignment_pipeline/Industrial_and_Scientific_clean/'
TBLPROPERTIES ("skip.header.line.count"="1");


## STEP 2, Columns review
## once the table is created and the data is stored on it, we continue
# 2.1 QUERY ONE: count total reviews:
SELECT COUNT(*) AS total_reviews FROM industrial_reviews;
# Output:
# OK
# 5183005
# Time taken: 204.136 seconds, Fetched: 1 row(s)

# 2.2 QUERY TWO: Count verified purchases
SELECT COUNT(*) AS verified_count
FROM industrial_reviews
WHERE verified_purchase = TRUE;
# Output:
# OK
# 3181039
# Time taken: 70.635 seconds, Fetched: 1 row(s)

# 2.3 QUERY THREE: we check the average rating:
SELECT AVG(rating) AS avg_rating FROM industrial_reviews;
# Output:
#OK
#4.183227297677699
#Time taken: 61.379 seconds, Fetched: 1 row(s)

# 2.4 QUERY FOUR: we confirm what is the max and min value from rating:
SELECT MAX(rating) AS max_rating, MIN(rating) AS min_rating
FROM industrial_reviews;
# Output:
#OK
#5.0     1.0
#Time taken: 63.783 seconds, Fetched: 1 row(s)

# 2.5 QUERY FIVE: we confirm the number of different products
SELECT parent_asin, COUNT(*) AS review_count
FROM industrial_reviews
GROUP BY parent_asin
ORDER BY review_count DESC
LIMIT 10;

# Output: 
#OK
#B0B6D1KQNJ      18097
#B09KZ6TBNY      15842
#B08WY4PXLT      11291
#B0B6YT7HNF      11055
#B07GGZT8S7      9823
#B0BGXZLXNJ      9792
#B0BGQR74H1      8943
#B09WW4Z413      8387
#B007Q2M17K      8126
#B098BR67DJ      7597
#Time taken: 125.424 seconds, Fetched: 10 row(s)

# 2.6 QUERY SIX: We check the values of helpful votes:
SELECT MAX(helpful_vote) AS max_helpful_vote, MIN(helpful_vote) AS min_helpful_vote
FROM industrial_reviews;
# Output:
# OK
# 5611    -1
# Time taken: 83.34 seconds, Fetched: 1 row(s)

# 2.7 QUERY SEVEN: logarithmic binning is applied to check its distribution, adding a negative value column to check, which might be an outlier:
SELECT
  CASE
    WHEN helpful_vote < 0 THEN '-1'
    WHEN helpful_vote = 0 THEN '0'
    WHEN helpful_vote BETWEEN 1 AND 10 THEN '1–10'
    WHEN helpful_vote BETWEEN 11 AND 100 THEN '11–100'
    WHEN helpful_vote BETWEEN 101 AND 1000 THEN '101–1,000'
    ELSE '1,001+'
  END AS vote_range,
  COUNT(*) AS count
FROM industrial_reviews
GROUP BY
  CASE
    WHEN helpful_vote < 0 THEN '-1'
    WHEN helpful_vote = 0 THEN '0'
    WHEN helpful_vote BETWEEN 1 AND 10 THEN '1–10'
    WHEN helpful_vote BETWEEN 11 AND 100 THEN '11–100'
    WHEN helpful_vote BETWEEN 101 AND 1000 THEN '101–1,000'
    ELSE '1,001+'
  END
ORDER BY count DESC;

# Output:
# OK
# -1      	1
# 0       	3991975
# 1–10    	1120393
# 11–100  	67083
# 101–1,000       3478
# 1,001+  75

# Time taken: 117.776 seconds, Fetched: 6 row(s)

# 2.8 QUERY EIGHT: Preview of Summary Statistics:
SELECT
  main_category,
  COUNT(*) AS total_valid_reviews,
  ROUND(AVG(rating), 2) AS avg_rating,
  MAX(helpful_vote) AS max_votes,
  ROUND(AVG(price), 2) AS avg_price
FROM industrial_reviews
WHERE main_category IS NOT NULL
  AND rating IS NOT NULL
  AND helpful_vote IS NOT NULL
  AND price IS NOT NULL
GROUP BY main_category
HAVING COUNT(*) > 1000
ORDER BY avg_rating DESC
LIMIT 10;

#This output showed: 
# OK
# Automotive      		            2905    4.41    92      1989392.04
# Health & Personal Care        	4079    4.35    63      48.23
# Industrial & Scientific 	      48960   4.35    394     5.26767179746E9
# Tools & Home Improvement        13406   4.32    216     2.30912479463E9
# Amazon Home     		            11558   4.32    160     625671.28
#         			                  1264    4.26    109     4.793617410743E10
# Office Products 		            1733    3.99    145     53.36
# true              			        4268    3.8     458     5.416726684E7
# Time taken: 167.57 seconds, Fetched: 8 row(s)

# It shows weird results, demonstrating outlier results and wrong formatted cells.

# 2.8.1 QUERY 8.1: We will create a better view filtering those wrong values out:
SELECT
  main_category,
  COUNT(*) AS total_valid_reviews,
  ROUND(AVG(rating), 2) AS avg_rating,
  MAX(helpful_vote) AS max_votes,
  ROUND(AVG(price), 2) AS avg_price
FROM industrial_reviews
WHERE main_category IS NOT NULL
  AND main_category != ''
  AND main_category NOT LIKE 'true'
  AND rating IS NOT NULL
  AND helpful_vote IS NOT NULL
  AND price IS NOT NULL
  AND price BETWEEN 1 AND 10000 -- realistic range
GROUP BY main_category
HAVING COUNT(*) > 500
ORDER BY total_valid_reviews DESC;

# Output:

# OK
# Industrial & Scientific 47916   4.35    394     88.1
# Tools & Home Improvement        13102   4.32    216     77.35
# Amazon Home     11437   4.32    160     81.93
# Health & Personal Care  4074    4.35    63      23.5
# Automotive      2871    4.41    92      168.01
# Office Products 1724    3.99    145     53.64
# All Beauty      992     3.88    44      19.11
# Five Stars      902     4.98    26      1194.54
# All Electronics 588     4.18    6       99.38
# Sports & Outdoors       559     4.55    37      28.82
# AMAZON FASHION  548     4.18    26      28.94
# Time taken: 135.466 seconds, Fetched: 11 row(s)

# 2.9 QUERY 9: Then we categorize prices per buckets:
SELECT
  CASE
    WHEN price < 10 THEN 'Under $10'
    WHEN price BETWEEN 10 AND 50 THEN '$10-50'
    WHEN price BETWEEN 50 AND 100 THEN '$50-100'
    WHEN price BETWEEN 100 AND 500 THEN '$100-500'
    WHEN price > 500 THEN 'Over $500'
    ELSE 'Unknown'
  END AS price_range,
  COUNT(*) AS total_products,
  ROUND(AVG(rating), 2) AS avg_rating
FROM industrial_reviews
WHERE price IS NOT NULL AND rating IS NOT NULL
  AND price BETWEEN 1 AND 10000
GROUP BY
  CASE
    WHEN price < 10 THEN 'Under $10'
    WHEN price BETWEEN 10 AND 50 THEN '$10-50'
    WHEN price BETWEEN 50 AND 100 THEN '$50-100'
    WHEN price BETWEEN 100 AND 500 THEN '$100-500'
    WHEN price > 500 THEN 'Over $500'
    ELSE 'Unknown'
  END
ORDER BY total_products DESC;

# Output:
# OK
# Under $10       158442  3.94
# Over $500       87331   3.97
# $10-50  76096   4.22
# $100-500        55179   4.01
# $50-100 24089   4.07
# Time taken: 115.46 seconds, Fetched: 5 row(s)
