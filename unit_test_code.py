from __future__ import print_function
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)
        
    spark = SparkSession.builder.appName("GCSSparkDEMO").getOrCreate()
        
    gsma_file_path = sys.argv[1]
    radio_file_path = sys.argv[2]
    sink_path = sys.argv[3]
    

    # declaring a schema for the csv file to prevent spark from creating an extra job to infer a schema
    gsmaSchema = "device_tac INTEGER, device_type STRING, device_live_since_date STRING, device_supported_technologies STRING, device_manufacturer STRING, device_model STRING"
    
    # reading the GSMA file
    gsmaDF = spark.read.csv(gsma_file_path, header=True, sep=";", schema=gsmaSchema)


    bqSchema = StructType([StructField("device_tac",IntegerType(),True),
        StructField("date_time_stamp",StringType(),True),
        StructField("energyconsumption",IntegerType(),True),
        StructField("trafficlevel",DecimalType(10,2),True)])

    radioDF = spark.read.csv(radio_file_path, header=True, schema=bqSchema)
    
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


    # selecting the worst 10 devices of each week, you can filter by year to limit the results
    windowSpec = Window.partitionBy("weekyear").orderBy("performance")
    outputDF = joinedDF.withColumn("row_id",row_number().over(windowSpec)).select("*").where(col("row_id")<=10)

    # writing the outputDF to GCS where it will be used as an external BigQuery table
    outputDF.write.format("parquet").partitionBy("year").option("path",sink_path).save()
    outputDF.show()

    spark.stop()
