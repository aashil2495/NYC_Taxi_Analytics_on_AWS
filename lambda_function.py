import requests
import boto3
import os
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
    
def lambda_handler(event, context):
    
    
    default_file_url='https://d37ci6vzurychx.cloudfront.net/trip-dat/'
    yellow_taxi_url_part='yellow_tripdata_'
    green_taxi_url_part='green_tripdata_'
    

    data_year_string=str(event['year'])
    data_month_string=str(event['month'])
    # data_year_string='2024'
    # data_month_string='12'
    
    
    
    
    yellow_taxi_fullurl=default_file_url+yellow_taxi_url_part+data_year_string+'-'+data_month_string+".parquet"
    green_taxi_fullurl=default_file_url+green_taxi_url_part+data_year_string+'-'+data_month_string+".parquet"

    
    
    s3=boto3.client('s3')
    bucket="staging-trip-data"
    status_chk_out=''
    def check_downloadable_url(bucket,url,filename):
        print("bucket: "+bucket)
        print("url: "+url)
        print("filename: "+filename)
        status_chk=''
        filefullpath=""
        filefullpath=f"{data_year_string}/{data_month_string}/"
        response = requests.head(url)
        print(response, "resp")
        if response.status_code==200:
            print("valid url")
            x=requests.get(url)
            s3.put_object(Bucket=bucket,Key=filefullpath+filename,Body=x.content)
            status_chk="1"
        else:
            print("invalid url")
            status_chk='0'
        return status_chk
    
    status_chk_out+=check_downloadable_url(bucket,yellow_taxi_fullurl,yellow_taxi_url_part+data_year_string+'-'+data_month_string)
    status_chk_out+=check_downloadable_url(bucket,green_taxi_fullurl,green_taxi_url_part+data_year_string+'-'+data_month_string)
    
    
    body={}
    
    if status_chk_out=='11':
        body['result']='success'
        return body
    else:
        body['result']='fail'
        return body
    
