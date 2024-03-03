# GCP-Spark-Demo

## Overview
Hello, this is, as the name implies, a GCP Spark demo to be executed on Dataproc.
In this demo, we read data from two sources (GCS and BigQuery), apply some transformations and save the output in Parquet format to be used later by an external Bigquery partitioned table.

## Contents
- Sample data (the sample data used in the unit test code).
- DDL CREATE statement for the BigQuery partitioned table.
- Project configuration JSON file (add your own parameters before submitting the spark job).
- GCP Spark Job PySpark script (the main Spark implementation of the use case).
- GCP Spark Job Pyspark script (an optimized version of the Spark implementation).
- Unit test code (to test the script on your machine)
- Unit test code manual validation excel sheet.
- Two snapshots showing the difference between using Broadcast join and Sort-Merge join.

## Assumption(s)
- Since BigQuery is GCP's implementation of Data Warehouse , I assumed that there will be history records, that's why I concatinated the week and year. For example to have an overview about how manufacturers have optimized their phones' performance over the weeks/years or for example to observe if performance degrades at certain times during the year. If I use weeks only, I will be aggregating/accumulating data to the same week of year each year. I couldn't imagine a business use case for having no history.

## Data Partitioning Policy
- I chose to partition the output on GCS by year, because this way, I believe the partitions will have equal sizes and it will make querying the table easier by filtering on specific year(s).

## Optimization
Since we are joining two large dataframes, I thought I would use the Sort-Merge join after eliminating the shuffle operation, which is expensive and requires partitions to be shuffled across the network between executers.
The idea create partitioned buckets for common sorted keys or columns on which we want to perform frequent equijoins. That is, we can create an explicit number of buckets to store specific sorted columns (one key per bucket, not quite exactly what I did, but the concept is still correct). Presorting and reorganizing data in this way boosts performance, as it allows us to skip the expensive Exchange operation and go straight to WholeStageCodegen.

In the below picture, I am using the default Broadcast join to join the two dataframes. Note the Exchange Hashpartitioning (shuffle operation).
![Exchange-HashPartitioning-Default-Broadcast-Join](https://github.com/eslam-mrc/GCP-Spark-Demo/assets/75180981/e31e9b06-cb38-4fa9-9ab5-5c25851a7398)

While in the below picture, notice how the Exchange Hashpartitioning no longer exists.
![SortMergeJoin-No Exchange Hashpartitioning](https://github.com/eslam-mrc/GCP-Spark-Demo/assets/75180981/87f91716-ea0f-499c-bdf6-6e3b0874050f)

## Dataproc Configuration
- gcloud dataproc clusters create cluster-name
--master-machine-type= n1-standard-16
--worker-machine-type= n1-standard-16
--master-boot-disk-type= pd-ssd
--master-boot-disk-size-gb = 100
--region=region
--num-workers=10

## Modules and Libraries Used
- The pyspark module includes the SparkContext and SQLContext.
- pyspark.sql.functions is a module in the PySpark library that provides a collection of built-in functions for working with structured data in Spark SQL and DataFrame API. I used functions like to_date(), concat() and sum().
- pyspark.sql.types is a module in PySpark that provides classes representing data types used in Spark SQL and DataFrame API. These data types are used to define the schema of DataFrames, specifying the structure and types of columns within a DataFrame. I used simple data types like IntegerType(), StringType() and DeciamlType(), as well as complex data types like StructType().
- JSON module for encoding and decoding JSON data.

## Note
- I don't have access to Dataproc but I ran and tested the unit test code and validated the output manually using the sample data.
You will find the validation sheet also attached.
- For the unit testing, I used Spark 2.4.7 along with python 2.7.5

## Resources
001 
Read files from Google Cloud Storage Bucket using local PySpark and Jupyter Notebooks
https://kashif-sohail.medium.com/read-files-from-google-cloud-storage-bucket-using-local-pyspark-and-jupyter-notebooks-f8bd43f4b42e

002 
Pyspark Project Setup for connecting Google BigQuery
https://l.facebook.com/l.php?u=https%3A%2F%2Fgrkamarnath.medium.com%2Fpyspark-project-setup-for-connecting-google-bigquery-2488ea721cc9%3Ffbclid%3DIwAR1XL9BVDmyrHs6rT5J9e5iJCN-G6FfctMYT_1laOAQEaB5KnvZ_MiiVwgM&h=AT1JljcXhRUPuj5AYDfyraA5z_oeKKG2uU0MuaWkIiiXUFNJMDoD5ho_lerINVhFPG6IeF8W6IgDG_xBdHyW6sNWvH9LWF_7nfd5hBO60oHxEGKTh9Qgm1XE7rUr0q8ED-0

003
Google Cloud Documentation
https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_external_table_statement

004
Pyspark Window Functions
https://sparkbyexamples.com/pyspark/pyspark-window-functions/

005
Chapter 7: Optimizing and Tuning Spark Applications: A Family of Spark Joins
O'REILLY Learning Spark 2nd Edition


