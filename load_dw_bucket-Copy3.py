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


  source_bucket, target_bucket = "staging-trip-data", "processed-trip-data"
  
  # fileyear,filemonth='2023','10'


  yellow_all_file_paths="s3://" + source_bucket + "/" + fileyear + "/" + \
                            filemonth + "/yellow_tripdata_" + fileyear + \
                            "-" + filemonth 
  green_all_file_paths="s3://" + source_bucket + "/" + fileyear + "/" +  \
                            filemonth + "/green_tripdata_" + fileyear + \
                            "-" + filemonth
  highvol_all_file_paths="s3://" + source_bucket + "/" + fileyear + "/" + \
                            filemonth + "/fhvhv_tripdata_" + fileyear +  \
                            "-" + filemonth


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

  fhvhv_trip_data= spark.read.parquet(highvol_all_file_paths)

  highvolume_fhvdf2=fhvhv_trip_data.withColumn("pickup_year", year(col("pickup_datetime"))) \
                        .withColumn("pickup_month", month(col("pickup_datetime"))) \
                        .withColumn("pickup_day", dayofmonth(col("pickup_datetime"))) \
                        .withColumn("pickup_hour", hour(col("pickup_datetime"))) \
                        .withColumn("pickup_minute", minute(col("pickup_datetime"))) \
                        .withColumn("dropoff_year", year(col("dropoff_datetime"))) \
                        .withColumn("dropoff_month", month(col("dropoff_datetime"))) \
                        .withColumn("dropoff_day", dayofmonth(col("dropoff_datetime"))) \
                        .withColumn("dropoff_hour", hour(col("dropoff_datetime"))) \
                        .withColumn("dropoff_minute", minute(col("dropoff_datetime")))

  highvolume_fhvdf2=highvolume_fhvdf2.withColumn('trip_distance',col('trip_miles'))

  highvolume_fhvdf3=highvolume_fhvdf2.withColumn("trip_time",col('trip_time') / 60)

  highvolume_fhvdf4=highvolume_fhvdf3.drop("request_datetime").drop("on_scene_datetime") \
                              .drop("pickup_datetime").drop("dropoff_datetime")\
                      .drop("access_a_ride_flag")

  highvolume_fhvdf5=highvolume_fhvdf4.withColumn("trip_time", col("trip_time").cast("integer"))

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

  fact_highvolume_pickupjoin=highvolume_fhvdf5.join(dim_date, (highvolume_fhvdf5['pickup_year'] == dim_date['year']) \
                              & (highvolume_fhvdf5['pickup_month'] == dim_date['month'] ) \
                    & (highvolume_fhvdf5['pickup_day'] == dim_date['day']), 'inner') 

  columns_to_drop = ['pickup_year', 'pickup_month','pickup_day','full_date','year','quarter','month','day','day_of_week',
                    'week_number','isweekend','isweekday'] 
  fact_highvolume_pickupjoin=fact_highvolume_pickupjoin.withColumnRenamed("DateId", "pickup_DateId").drop(*columns_to_drop)

  fact_highvolume_dropoffjoin=fact_highvolume_pickupjoin. \
                      join(dim_date, (fact_highvolume_pickupjoin['dropoff_year'] == dim_date['year']) \
                      & (fact_highvolume_pickupjoin['dropoff_month'] == dim_date['month']) \
                    & (fact_highvolume_pickupjoin['dropoff_day'] == dim_date['day']), 'inner')

  columns_to_drop = ['dropoff_year', 'dropoff_month','dropoff_day','full_date','year','quarter','month','day','day_of_week',
                    'week_number','isweekend','isweekday']
  fact_highvolume_dropoffjoin=fact_highvolume_dropoffjoin.withColumnRenamed("DateId", "dropoff_DateId").drop(*columns_to_drop)

  fact_highvolume_pickuptimejoin=fact_highvolume_dropoffjoin. \
                      join(dim_time, (fact_highvolume_dropoffjoin['pickup_hour'] == dim_time['hour']) \
                      & (fact_highvolume_dropoffjoin['pickup_minute'] == dim_time['minute']) , 'inner')

  columns_to_drop = ['pickup_hour', 'pickup_minute','hour','minute']
  fact_highvolume_pickuptimejoin=fact_highvolume_pickuptimejoin.withColumnRenamed("TimeId", "pickup_TimeId").drop(*columns_to_drop)

  fact_highvolume_dropofftimejoin=fact_highvolume_pickuptimejoin. \
                      join(dim_time, (fact_highvolume_pickuptimejoin['dropoff_hour'] == dim_time['hour']) \
                      & (fact_highvolume_pickuptimejoin['dropoff_minute'] == dim_time['minute']) , 'inner')

  columns_to_drop = ['dropoff_hour', 'dropoff_minute','hour','minute']
  fact_highvolume_dropofftimejoin=fact_highvolume_dropofftimejoin.withColumnRenamed("TimeId", "dropoff_TimeId").drop(*columns_to_drop)

  fact_highvolume_flagsjoin=fact_highvolume_dropofftimejoin.join(dim_flags, (highvolume_fhvdf5['shared_request_flag'] == dim_flags['SharedRequestFlag']) \
                              & (highvolume_fhvdf5['shared_match_flag'] == dim_flags['SharedMatchFlag'] ) \
                          & (highvolume_fhvdf5['wav_request_flag'] == dim_flags['WavRequestFlag'] ) \
                            & (highvolume_fhvdf5['wav_match_flag'] == dim_flags['WavMatchFlag']), 'inner') 

  columns_to_drop = ['shared_request_flag', 'shared_match_flag','wav_request_flag','wav_match_flag','SharedRequestFlag'
                    ,'SharedMatchFlag','WavRequestFlag','WavMatchFlag']
  fact_highvolume_flagsjoin=fact_highvolume_flagsjoin.withColumnRenamed("TimeId", "dropoff_TimeId").drop(*columns_to_drop)

  fact_highvolume_flagsjoin=fact_highvolume_flagsjoin.withColumnRenamed("id", "FlagId")

  fact_yellow_dropofftimejoin=fact_yellow_dropofftimejoin.withColumn("data_year",lit(fileyear)).withColumn("data_month",lit(filemonth))
  fact_yellow_dropofftimejoin = fact_yellow_dropofftimejoin.withColumn("fare_amount", when(col("fare_amount") < 0, col("fare_amount") * -1).otherwise(col("fare_amount")))
  fact_yellow_dropofftimejoin = fact_yellow_dropofftimejoin.withColumn("extra", when(col("extra") < 0, col("extra") * -1).otherwise(col("extra")))
  fact_yellow_dropofftimejoin = fact_yellow_dropofftimejoin.withColumn("mta_tax", when(col("mta_tax") < 0, col("mta_tax") * -1).otherwise(col("mta_tax")))
  fact_yellow_dropofftimejoin = fact_yellow_dropofftimejoin.withColumn("tip_amount", when(col("tip_amount") < 0, col("tip_amount") * -1).otherwise(col("tip_amount")))
  fact_yellow_dropofftimejoin = fact_yellow_dropofftimejoin.withColumn("tolls_amount", when(col("tolls_amount") < 0, col("tolls_amount") * -1).otherwise(col("tolls_amount")))
  fact_yellow_dropofftimejoin = fact_yellow_dropofftimejoin.withColumn("improvement_surcharge", when(col("improvement_surcharge") < 0, col("improvement_surcharge") * -1).otherwise(col("improvement_surcharge")))
  fact_yellow_dropofftimejoin = fact_yellow_dropofftimejoin.withColumn("total_amount", when(col("total_amount") < 0, col("total_amount") * -1).otherwise(col("total_amount")))
  fact_yellow_dropofftimejoin = fact_yellow_dropofftimejoin.withColumn("congestion_surcharge", when(col("congestion_surcharge") < 0, col("congestion_surcharge") * -1).otherwise(col("congestion_surcharge")))
  fact_yellow_dropofftimejoin = fact_yellow_dropofftimejoin.withColumn("airport_fee", when(col("airport_fee") < 0, col("airport_fee") * -1).otherwise(col("airport_fee")))
  
  fact_green_dropofftimejoin=fact_green_dropofftimejoin.withColumn("data_year",lit(fileyear)).withColumn("data_month",lit(filemonth))
  fact_green_dropofftimejoin=fact_green_dropofftimejoin.withColumn("fare_amount", when(col("fare_amount") < 0, col("fare_amount") * -1).otherwise(col("fare_amount")))
  fact_green_dropofftimejoin = fact_green_dropofftimejoin.withColumn("extra", when(col("extra") < 0, col("extra") * -1).otherwise(col("extra")))
  fact_green_dropofftimejoin = fact_green_dropofftimejoin.withColumn("mta_tax", when(col("mta_tax") < 0, col("mta_tax") * -1).otherwise(col("mta_tax")))
  fact_green_dropofftimejoin = fact_green_dropofftimejoin.withColumn("tip_amount", when(col("tip_amount") < 0, col("tip_amount") * -1).otherwise(col("tip_amount")))
  fact_green_dropofftimejoin = fact_green_dropofftimejoin.withColumn("tolls_amount", when(col("tolls_amount") < 0, col("tolls_amount") * -1).otherwise(col("tolls_amount")))
  fact_green_dropofftimejoin = fact_green_dropofftimejoin.withColumn("improvement_surcharge", when(col("improvement_surcharge") < 0, col("improvement_surcharge") * -1).otherwise(col("improvement_surcharge")))
  fact_green_dropofftimejoin = fact_green_dropofftimejoin.withColumn("total_amount", when(col("total_amount") < 0, col("total_amount") * -1).otherwise(col("total_amount")))
  fact_green_dropofftimejoin = fact_green_dropofftimejoin.withColumn("congestion_surcharge", when(col("congestion_surcharge") < 0, col("congestion_surcharge") * -1).otherwise(col("congestion_surcharge")))

  fact_highvolume_flagsjoin=fact_highvolume_flagsjoin.withColumn("data_year",lit(fileyear)).withColumn("data_month",lit(filemonth))
  fact_highvolume_flagsjoin=fact_highvolume_flagsjoin.withColumn("base_passenger_fare", when(col("base_passenger_fare") < 0, col("base_passenger_fare") * -1).otherwise(col("base_passenger_fare")))
  fact_highvolume_flagsjoin = fact_highvolume_flagsjoin.withColumn("tolls", when(col("tolls") < 0, col("tolls") * -1).otherwise(col("tolls")))
  fact_highvolume_flagsjoin = fact_highvolume_flagsjoin.withColumn("bcf", when(col("bcf") < 0, col("bcf") * -1).otherwise(col("bcf")))
  fact_highvolume_flagsjoin = fact_highvolume_flagsjoin.withColumn("sales_tax", when(col("sales_tax") < 0, col("sales_tax") * -1).otherwise(col("sales_tax")))
  fact_highvolume_flagsjoin = fact_highvolume_flagsjoin.withColumn("congestion_surcharge", when(col("congestion_surcharge") < 0, col("congestion_surcharge") * -1).otherwise(col("congestion_surcharge")))
  fact_highvolume_flagsjoin = fact_highvolume_flagsjoin.withColumn("airport_fee", when(col("airport_fee") < 0, col("airport_fee") * -1).otherwise(col("airport_fee")))
  fact_highvolume_flagsjoin = fact_highvolume_flagsjoin.withColumn("tips", when(col("tips") < 0, col("tips") * -1).otherwise(col("tips")))
  fact_highvolume_flagsjoin = fact_highvolume_flagsjoin.withColumn("driver_pay", when(col("driver_pay") < 0, col("driver_pay") * -1).otherwise(col("driver_pay")))


  dw_bucket_fact_path="s3://"+target_bucket+"/Fact/"
  yellowfile,greenfile,highvolfile='yellow/','green/','highvol/'

  fact_yellow_dropofftimejoin.repartition(1).write.mode("overwrite").option("header","true").csv(dw_bucket_fact_path+yellowfile+fileyear+"/"+filemonth+"/")
  fact_green_dropofftimejoin.repartition(1).write.mode("overwrite").option("header","true").csv(dw_bucket_fact_path+greenfile+fileyear+"/"+filemonth+"/")
  fact_highvolume_flagsjoin.repartition(1).write.mode("overwrite").option("header","true").csv(dw_bucket_fact_path+highvolfile+fileyear+"/"+filemonth+"/")
#   filelist=list(os.listdir(dw_bucket_fact_path+highvolfile+fileyear+"/"+filemonth+"/"))
#   os.rename(dw_bucket_fact_path+highvolfile+fileyear+"/"+filemonth+"/"+filelist[0],dw_bucket_fact_path+highvolfile+fileyear+"/"+filemonth+"/"+"highvol.csv")









