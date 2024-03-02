#!/usr/bin/env python

# imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
import json

# reading the configuration file
with open('project_configs.json') as f:
    configs_file = json.load(f)

# creating a spark session
spark = SparkSession.builder.appName("GCPSparkJob").getOrCreate()

path_to_json_credentials_file = configs_file["path_to_your_credentials_json"]
spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",path_to_json_credentials_file)

# declaring a schema for the csv file to prevent spark from creating an extra job to infer a schema
gsmaSchema = "device_tac INTEGER, device_type STRING, device_live_since_date STRING, device_supported_technologies STRING, device_manufacturer STRING, device_model STRING"

# reading the GSMA file
gcs_bucket = configs_file["bucket_name"]
object_key = configs_file["object_key"]
file_path = f"gs://{gcs_bucket}/{object_key}"
gsmaDF = spark.read.csv(file_path, header=True, sep=";", schema=gsmaSchema)

# declaring a schema for the BigQuery table
bqSchema = StructType([StructField("device_tac",IntegerType(),True),
    StructField("date_time_stamp",StringType(),True),
    StructField("energyconsumption",IntegerType(),True),
    StructField("trafficlevel",DecimalType(10,2),True)])

# reading the BigQuery table
gcp_project_id = configs_file["gcp_project_id"]
bq_dataset_name = configs_file["bigquery_dataset_name"]
bq_table_name = configs_file["bigquery_table_name"]
radioDF = spark.read.format("bigquery")
    .option("table",f"{gcp_project_id}.{bq_dataset_name}.{bq_table_name}")
    .schema(bqSchema)
    .load()
    
# adding weekyear, year columns to the radioDF
radioDF = (radioDF.withColumn("dt",to_date("date_time_stamp","yyyy/mm/dd")).
    drop("date_time_stamp").
    withColumn("week",weekofyear("dt")).
    withColumn("year", year("dt")).
    withColumn("weekyear", concat(col("week"),lit("-"),col("year"))).
    drop("dt").
    drop("week"))
    
# preparing the radioDF to be joined with gsmaDF by aggregating energy consumption, traffic level 
# and calculating performance
radioDF = (radioDF.groupBy("device_tac","weekyear","year").
    agg(sum("energyconsumption").alias("energy_consumption"),sum("trafficlevel").alias("traffic_lvl")).
    withColumn("performance", col("traffic_lvl")/col("energy_consumption")))
    

# joining the radioDF and gsmaDF while selecting the needed columns only
joinedDF = radioDF.join(gsmaDF, "device_tac").select(radioDF["*"],gsmaDF["device_manufacturer"])


# selecting the worst 10 devices of each week, you can filter by year to limit the resultset
windowSpec = Window.partitionBy("weekyear").orderBy("performance")
outputDF = joinedDF.withColumn("row_id",row_number().over(windowSpec)).select("*").where(col("row_id")<=10)

# writing the outputDF to GCS where it will be used as an external BigQuery table
sink_path = configs_file["gcs_sink_path"]
outputDF.write.format("parquet").partitionBy("year").option("path",sink_path).save()

spark.stop()

    









