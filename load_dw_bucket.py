from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime as dt
import sys
import os

if __name__=="__main__":

  yellow_all_file_paths=""
  green_all_file_paths=""
  highvol_all_file_paths=""
  spark = SparkSession.builder.appName("test").getOrCreate()
  fileyear = sys.argv[1]
  filemonth = sys.argv[2]
  print(fileyear)
  print(filemonth)
  source_bucket, target_bucket = "staging-trip-data", "processed-trip-data"
  
  # fileyear,filemonth='2023','10'


  yellow_all_file_paths="s3://" + source_bucket + "/" + fileyear + "/" + \
                            filemonth + "/yellow_tripdata_" + fileyear + \
                            "-" + filemonth 
  green_all_file_paths="s3://" + source_bucket + "/" + fileyear + "/" +  \
                            filemonth + "/green_tripdata_" + fileyear + \
                            "-" + filemonth
  # highvol_all_file_paths="s3://" + source_bucket + "/" + fileyear + "/" + \
  #                           filemonth + "/fhvhv_tripdata_" + fileyear +  \
  #                           "-" + filemonth


  yellow_trip_data = spark.read.parquet(yellow_all_file_paths)

  yellowdf2=yellow_trip_data.withColumn("pickup_year", year(col("tpep_pickup_datetime"))) \
                        .withColumn("pickup_month", month(col("tpep_pickup_datetime"))) \
                        .withColumn("pickup_day", dayofmonth(col("tpep_pickup_datetime"))) \
                        .withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
                        .withColumn("pickup_minute", minute(col("tpep_pickup_datetime"))) \
                        .withColumn("dropoff_year", year(col("tpep_dropoff_datetime"))) \
                        .withColumn("dropoff_month", month(col("tpep_dropoff_datetime"))) \
                        .withColumn("dropoff_day", dayofmonth(col("tpep_dropoff_datetime"))) \
                        .withColumn("dropoff_hour", hour(col("tpep_dropoff_datetime"))) \
                        .withColumn("dropoff_minute", minute(col("tpep_dropoff_datetime")))

  yellowdf3=yellowdf2.withColumn("unix_ts_1", unix_timestamp(col("tpep_pickup_datetime"))) \
                              .withColumn("unix_ts_2", unix_timestamp(col("tpep_dropoff_datetime")))

  yellowdf3=yellowdf3.withColumn("trip_time",(col("unix_ts_2") - col("unix_ts_1")) / 60)

  yellowdf4=yellowdf3.drop("tpep_pickup_datetime").drop("tpep_dropoff_datetime").drop("unix_ts_1").drop("unix_ts_2").drop("store_and_fwd_flag")

  yellowdf4=yellowdf4.withColumn("passenger_count", col("passenger_count").cast("integer")) \
              .withColumn("RatecodeID", col("RatecodeID").cast("integer")) \
              .withColumn("trip_time", col("trip_time").cast("integer")) 

  yellowdf4 = yellowdf4.filter(yellowdf4.total_amount >= 0)

  green_trip_data = spark.read.parquet(green_all_file_paths)

  greendf2=green_trip_data.withColumn("pickup_year", year(col("lpep_pickup_datetime"))) \
                        .withColumn("pickup_month", month(col("lpep_pickup_datetime"))) \
                        .withColumn("pickup_day", dayofmonth(col("lpep_pickup_datetime"))) \
                        .withColumn("pickup_hour", hour(col("lpep_pickup_datetime"))) \
                        .withColumn("pickup_minute", minute(col("lpep_pickup_datetime"))) \
                        .withColumn("dropoff_year", year(col("lpep_dropoff_datetime"))) \
                        .withColumn("dropoff_month", month(col("lpep_dropoff_datetime"))) \
                        .withColumn("dropoff_day", dayofmonth(col("lpep_dropoff_datetime"))) \
                        .withColumn("dropoff_hour", hour(col("lpep_dropoff_datetime"))) \
                        .withColumn("dropoff_minute", minute(col("lpep_dropoff_datetime")))

  greendf3=greendf2.withColumn("unix_ts_1", unix_timestamp(col("lpep_pickup_datetime"))) \
                              .withColumn("unix_ts_2", unix_timestamp(col("lpep_dropoff_datetime")))

  greendf3=greendf3.withColumn("trip_time",(col("unix_ts_2") - col("unix_ts_1")) / 60)

  greendf4=greendf3.drop("lpep_pickup_datetime").drop("lpep_dropoff_datetime").drop("unix_ts_1").drop("unix_ts_2") \
                      .drop("store_and_fwd_flag").drop('ehail_fee')

  greendf4=greendf4.withColumn("passenger_count", col("passenger_count").cast("integer")) \
              .withColumn("RatecodeID", col("RatecodeID").cast("integer")) \
              .withColumn("trip_time", col("trip_time").cast("integer")) \
              .withColumn("payment_type", col("payment_type").cast("integer")) \
              .withColumn("trip_type", col("trip_type").cast("integer"))

  columns_to_check = [
    "VendorID", "RatecodeID", "PULocationID", "DOLocationID", 
    "passenger_count", "trip_distance", "fare_amount", "extra", 
    "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", 
    "total_amount", "payment_type", "trip_type", "congestion_surcharge"
  ]
  greendf4 = greendf4.na.drop(subset=columns_to_check)
  greendf4=greendf4.filter(greendf4.total_amount >= 0)

  # fhvhv_trip_data= spark.read.parquet(highvol_all_file_paths)

  # highvolume_fhvdf2=fhvhv_trip_data.withColumn("pickup_year", year(col("pickup_datetime"))) \
  #                       .withColumn("pickup_month", month(col("pickup_datetime"))) \
  #                       .withColumn("pickup_day", dayofmonth(col("pickup_datetime"))) \
  #                       .withColumn("pickup_hour", hour(col("pickup_datetime"))) \
  #                       .withColumn("pickup_minute", minute(col("pickup_datetime"))) \
  #                       .withColumn("dropoff_year", year(col("dropoff_datetime"))) \
  #                       .withColumn("dropoff_month", month(col("dropoff_datetime"))) \
  #                       .withColumn("dropoff_day", dayofmonth(col("dropoff_datetime"))) \
  #                       .withColumn("dropoff_hour", hour(col("dropoff_datetime"))) \
  #                       .withColumn("dropoff_minute", minute(col("dropoff_datetime")))

  # highvolume_fhvdf2=highvolume_fhvdf2.withColumn('trip_distance',col('trip_miles'))

  # highvolume_fhvdf3=highvolume_fhvdf2.withColumn("trip_time",col('trip_time') / 60)

  # highvolume_fhvdf4=highvolume_fhvdf3.drop("request_datetime").drop("on_scene_datetime") \
  #                             .drop("pickup_datetime").drop("dropoff_datetime")\
  #                     .drop("access_a_ride_flag")

  # highvolume_fhvdf5=highvolume_fhvdf4.withColumn("trip_time", col("trip_time").cast("integer"))

#   file_name='dim_base'
#   dw_bucket_dimension_path="s3://"+target_bucket+"/Dimension/"
#   dim_base=spark.read.csv("s3://"+source_bucket+"/dim_base.csv",header=True)
#   dim_base=dim_base.dropDuplicates()
#   dim_base.repartition(1).write.mode("overwrite").option("header","true").csv(dw_bucket_dimension_path+file_name)

  dim_date=spark.read.csv("s3://processed-trip-data/Dimension/dim_date/part*",header=True)
  dim_time=spark.read.csv("s3://processed-trip-data/Dimension/dim_time/part*",header=True)
  dim_flags=spark.read.csv("s3://processed-trip-data/Dimension/dim_flags/part*",header=True)

  fact_yellow_pickupjoin=yellowdf4.join(dim_date, (yellowdf4['pickup_year'] == dim_date['year']) \
                              & (yellowdf4['pickup_month'] == dim_date['month'] ) \
                    & (yellowdf4['pickup_day'] == dim_date['day']), 'inner') 

  columns_to_drop = ['pickup_year', 'pickup_month','pickup_day','full_date','year','quarter','month','day','day_of_week',
                    'week_number','isweekend','isweekday'] 
  fact_yellow_pickupjoin=fact_yellow_pickupjoin.withColumnRenamed("DateId", "pickup_DateId").drop(*columns_to_drop)


  fact_yellow_dropoffjoin=fact_yellow_pickupjoin. \
                      join(dim_date, (fact_yellow_pickupjoin['dropoff_year'] == dim_date['year']) \
                      & (fact_yellow_pickupjoin['dropoff_month'] == dim_date['month']) \
                    & (fact_yellow_pickupjoin['dropoff_day'] == dim_date['day']), 'inner')

  columns_to_drop = ['dropoff_year', 'dropoff_month','dropoff_day','full_date','year','quarter','month','day','day_of_week',
                    'week_number','isweekend','isweekday']
  fact_yellow_dropoffjoin=fact_yellow_dropoffjoin.withColumnRenamed("DateId", "dropoff_DateId").drop(*columns_to_drop)

  fact_yellow_pickuptimejoin=fact_yellow_dropoffjoin. \
                      join(dim_time, (fact_yellow_dropoffjoin['pickup_hour'] == dim_time['hour']) \
                      & (fact_yellow_dropoffjoin['pickup_minute'] == dim_time['minute']) , 'inner')

  columns_to_drop = ['pickup_hour', 'pickup_minute','hour','minute']
  fact_yellow_pickuptimejoin=fact_yellow_pickuptimejoin.withColumnRenamed("TimeId", "pickup_TimeId").drop(*columns_to_drop)

  fact_yellow_dropofftimejoin=fact_yellow_pickuptimejoin. \
                      join(dim_time, (fact_yellow_pickuptimejoin['dropoff_hour'] == dim_time['hour']) \
                      & (fact_yellow_pickuptimejoin['dropoff_minute'] == dim_time['minute']) , 'inner')

  columns_to_drop = ['dropoff_hour', 'dropoff_minute','hour','minute']
  fact_yellow_dropofftimejoin=fact_yellow_dropofftimejoin.withColumnRenamed("TimeId", "dropoff_TimeId").drop(*columns_to_drop)


  fact_green_pickupjoin=greendf4.join(dim_date, (greendf4['pickup_year'] == dim_date['year']) \
                              & (greendf4['pickup_month'] == dim_date['month'] ) \
                    & (greendf4['pickup_day'] == dim_date['day']), 'inner') 

  columns_to_drop = ['pickup_year', 'pickup_month','pickup_day','full_date','year','quarter','month','day','day_of_week',
                    'week_number','isweekend','isweekday'] 
  fact_green_pickupjoin=fact_green_pickupjoin.withColumnRenamed("DateId", "pickup_DateId").drop(*columns_to_drop)

  fact_green_dropoffjoin=fact_green_pickupjoin. \
                      join(dim_date, (fact_green_pickupjoin['dropoff_year'] == dim_date['year']) \
                      & (fact_green_pickupjoin['dropoff_month'] == dim_date['month']) \
                    & (fact_green_pickupjoin['dropoff_day'] == dim_date['day']), 'inner')

  columns_to_drop = ['dropoff_year', 'dropoff_month','dropoff_day','full_date','year','quarter','month','day','day_of_week',
                    'week_number','isweekend','isweekday']
  fact_green_dropoffjoin=fact_green_dropoffjoin.withColumnRenamed("DateId", "dropoff_DateId").drop(*columns_to_drop)

  fact_green_pickuptimejoin=fact_green_dropoffjoin. \
                      join(dim_time, (fact_green_dropoffjoin['pickup_hour'] == dim_time['hour']) \
                      & (fact_green_dropoffjoin['pickup_minute'] == dim_time['minute']) , 'inner')

  columns_to_drop = ['pickup_hour', 'pickup_minute','hour','minute']
  fact_green_pickuptimejoin=fact_green_pickuptimejoin.withColumnRenamed("TimeId", "pickup_TimeId").drop(*columns_to_drop)

  fact_green_dropofftimejoin=fact_green_pickuptimejoin. \
                      join(dim_time, (fact_green_pickuptimejoin['dropoff_hour'] == dim_time['hour']) \
                      & (fact_green_pickuptimejoin['dropoff_minute'] == dim_time['minute']) , 'inner')

  columns_to_drop = ['dropoff_hour', 'dropoff_minute','hour','minute']
  fact_green_dropofftimejoin=fact_green_dropofftimejoin.withColumnRenamed("TimeId", "dropoff_TimeId").drop(*columns_to_drop)

  # fact_highvolume_pickupjoin=highvolume_fhvdf5.join(dim_date, (highvolume_fhvdf5['pickup_year'] == dim_date['year']) \
  #                             & (highvolume_fhvdf5['pickup_month'] == dim_date['month'] ) \
  #                   & (highvolume_fhvdf5['pickup_day'] == dim_date['day']), 'inner') 

  # columns_to_drop = ['pickup_year', 'pickup_month','pickup_day','full_date','year','quarter','month','day','day_of_week',
  #                   'week_number','isweekend','isweekday'] 
  # fact_highvolume_pickupjoin=fact_highvolume_pickupjoin.withColumnRenamed("DateId", "pickup_DateId").drop(*columns_to_drop)

  # fact_highvolume_dropoffjoin=fact_highvolume_pickupjoin. \
  #                     join(dim_date, (fact_highvolume_pickupjoin['dropoff_year'] == dim_date['year']) \
  #                     & (fact_highvolume_pickupjoin['dropoff_month'] == dim_date['month']) \
  #                   & (fact_highvolume_pickupjoin['dropoff_day'] == dim_date['day']), 'inner')

  # columns_to_drop = ['dropoff_year', 'dropoff_month','dropoff_day','full_date','year','quarter','month','day','day_of_week',
  #                   'week_number','isweekend','isweekday']
  # fact_highvolume_dropoffjoin=fact_highvolume_dropoffjoin.withColumnRenamed("DateId", "dropoff_DateId").drop(*columns_to_drop)

  # fact_highvolume_pickuptimejoin=fact_highvolume_dropoffjoin. \
  #                     join(dim_time, (fact_highvolume_dropoffjoin['pickup_hour'] == dim_time['hour']) \
  #                     & (fact_highvolume_dropoffjoin['pickup_minute'] == dim_time['minute']) , 'inner')

  # columns_to_drop = ['pickup_hour', 'pickup_minute','hour','minute']
  # fact_highvolume_pickuptimejoin=fact_highvolume_pickuptimejoin.withColumnRenamed("TimeId", "pickup_TimeId").drop(*columns_to_drop)

  # fact_highvolume_dropofftimejoin=fact_highvolume_pickuptimejoin. \
  #                     join(dim_time, (fact_highvolume_pickuptimejoin['dropoff_hour'] == dim_time['hour']) \
  #                     & (fact_highvolume_pickuptimejoin['dropoff_minute'] == dim_time['minute']) , 'inner')

  # columns_to_drop = ['dropoff_hour', 'dropoff_minute','hour','minute']
  # fact_highvolume_dropofftimejoin=fact_highvolume_dropofftimejoin.withColumnRenamed("TimeId", "dropoff_TimeId").drop(*columns_to_drop)

  # fact_highvolume_flagsjoin=fact_highvolume_dropofftimejoin.join(dim_flags, (highvolume_fhvdf5['shared_request_flag'] == dim_flags['SharedRequestFlag']) \
  #                             & (highvolume_fhvdf5['shared_match_flag'] == dim_flags['SharedMatchFlag'] ) \
  #                         & (highvolume_fhvdf5['wav_request_flag'] == dim_flags['WavRequestFlag'] ) \
  #                           & (highvolume_fhvdf5['wav_match_flag'] == dim_flags['WavMatchFlag']), 'inner') 

  # columns_to_drop = ['shared_request_flag', 'shared_match_flag','wav_request_flag','wav_match_flag','SharedRequestFlag'
  #                   ,'SharedMatchFlag','WavRequestFlag','WavMatchFlag']
  # fact_highvolume_flagsjoin=fact_highvolume_flagsjoin.withColumnRenamed("TimeId", "dropoff_TimeId").drop(*columns_to_drop)

  # fact_highvolume_flagsjoin=fact_highvolume_flagsjoin.withColumnRenamed("id", "FlagId")

  fact_yellow_dropofftimejoin=fact_yellow_dropofftimejoin.withColumn("data_year",lit(fileyear)).withColumn("data_month",lit(filemonth))
  fact_green_dropofftimejoin=fact_green_dropofftimejoin.withColumn("data_year",lit(fileyear)).withColumn("data_month",lit(filemonth))
  # fact_highvolume_flagsjoin=fact_highvolume_flagsjoin.withColumn("data_year",lit(fileyear)).withColumn("data_month",lit(filemonth))
  

  dw_bucket_fact_path="s3://"+target_bucket+"/Fact/"
  yellowfile,greenfile,highvolfile='yellow/','green/','highvol/'

  fact_yellow_dropofftimejoin.repartition(1).write.mode("overwrite").option("header","true").csv(dw_bucket_fact_path+yellowfile+fileyear+"/"+filemonth+"/")
  fact_green_dropofftimejoin.repartition(1).write.mode("overwrite").option("header","true").csv(dw_bucket_fact_path+greenfile+fileyear+"/"+filemonth+"/")
  # fact_highvolume_flagsjoin.repartition(1).write.mode("overwrite").option("header","true").csv(dw_bucket_fact_path+highvolfile+fileyear+"/"+filemonth+"/")










