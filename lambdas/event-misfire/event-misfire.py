from __future__ import print_function

import os, sys, re, json, requests, boto3
from datetime import tzinfo, timedelta, datetime, timezone
import logging
import ntpath
import elasticsearch
import traceback
from time import time
'''
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
'''

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"



if "SIGNAL_FILE_BUCKET" in os.environ:
    signal_file_bucket = os.environ["SIGNAL_FILE_BUCKET"]

if "MOZART_ES_URL" not in os.environ:
    raise RuntimeError("Need to specify MOZART_ES_URL in environment.")

MOZART_ES_URL = os.environ["MOZART_ES_URL"]
event_misfire_delay_threshold_second = int(os.environ["DELAY_THRESHOLD"])
event_misfire_metric_name = os.environ["E_MISFIRE_METRIC_ALARM_NAME"]

def send_cloudwatch_alarm(missed_file_count):
    import boto3

    # Create CloudWatch client
    cloudwatch = boto3.client('cloudwatch')

    response = cloudwatch.put_metric_data(
    MetricData = [
        {
            'MetricName': 'NumberOfMissedFiles',
            'Dimensions': [
                {
                    'Name': 'LAMBDA_NAME',
                    'Value': 'event-misfire_lambda'
                },
                {
                    'Name': 'E_MISFIRE_METRIC_ALARM_NAME',
                    'Value': event_misfire_metric_name
                },
            ],
            'Unit': 'Count',
            'Value': missed_file_count
        },
    ],
    Namespace='AWS/Lambda'
)

def get_s3_files(bucket_name, pref=""):
    s3 = boto3.client('s3')

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=pref)

    sorted_objects_dict = {}

    for page in pages:
        count = page['KeyCount']
        if count>0:
            for obj in page['Contents']:
                if not obj['Key'].endswith("/"):
                    sorted_objects_dict[obj['Key']] = obj['LastModified']

    return sorted_objects_dict

def get_job_info(file_name):
    no_of_jobs = 0
    ES = elasticsearch.Elasticsearch(MOZART_ES_URL)

    job_id = "ingest-staged-{}-*".format(file_name)
    query = { 
      "query": {
        "wildcard": {
          "job_id": {
            "value": job_id
          }
        }
      }
    }    
    es_header = {"Content-Type": "application/json"}
    q = json.dumps(query)
    print(q)
    try:
        result = ES.search(index="job_status-current", body=q) 
        no_of_jobs = len(result["hits"]["hits"])
    except Exception as err:
        print("ERROR Searching for job : {}".format(str(err)))
    print(no_of_jobs)
    if no_of_jobs>0:
        print(result["hits"]["hits"][0])
    return no_of_jobs
    

def lambda_handler(event, context):
    s3_files = {}
    missed_files = []
 
    #check if there any file left in signal_file_bucket
    s3_files = get_s3_files(signal_file_bucket)
    alert_msg = "Possible Event Misfire: There are ancillary files left in the bucket. Please check the information below and take immediate action"

    now = datetime.now(timezone.utc)
    if len(s3_files)==0:
        print("No files found in {}".format(signal_file_bucket))
    else:
        print("event_misfire_delay_threshold_second : {} seconds".format(event_misfire_delay_threshold_second))
        for key in s3_files:
            file_creation_time = s3_files[key]
            print("file_creation_time : {} of type : {}".format(file_creation_time, type(file_creation_time)))
            if type(file_creation_time) == str:
                file_creation_time = datetime.strptime(file_creation_time, '%d%m%YT%H:%M:%S')

            difference = (now - file_creation_time).total_seconds()        
            print("File was created {} seconds ago".format(difference))

            if difference <= event_misfire_delay_threshold_second:
                print("No Action as file creation time within threshold value of : {} seconds".format(event_misfire_delay_threshold_second))
                continue

            print("Checking job as diff is above threshold value of : {} seconds".format(event_misfire_delay_threshold_second))
            job_data_len = get_job_info(key)
            if job_data_len>0:
                print("Job Found, No action Required")
                continue
            else:
                print("No Job Found. Submit Alert")
                missed_files.append(key)

    missed_file_count = len(missed_files)
    print("missed_file_count : {}".format(missed_file_count))
    send_cloudwatch_alarm(missed_file_count)    

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
