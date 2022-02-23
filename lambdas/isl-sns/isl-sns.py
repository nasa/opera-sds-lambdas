from __future__ import print_function

import os, sys, re, json, requests, boto3
from datetime import datetime

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

print("Loading ISL Lambda function")

signal_file_suffix = None

if "SIGNAL_FILE_SUFFIX" in os.environ:
    signal_file_suffix = os.environ["SIGNAL_FILE_SUFFIX"]

if "MOZART_URL" not in os.environ:
    raise RuntimeError("Need to specify MOZART_URL in environment.")
MOZART_URL = os.environ["MOZART_URL"]
JOB_SUBMIT_URL = "%s/api/v0.1/job/submit" % MOZART_URL

def __get_job_type_info(data_file, job_types, default_type, default_release,
                   default_queue):
    """
    Determine the job type.
    :param data_file: The data file being ingested.
    :param job_types: A mapping of job types to a regex.
    :param default_type: Default job type.
    :param default_release: Default job release version.
    :param: default_queue: Default job queue.
    :return: Function will try to match the given data file to one of 
    the types specified in the job type mapping. If no mapping exists 
    or a match could not be found, then it will use the default job
    type, release, and queue.
    """
    for type in job_types.keys():
        regex = job_types[type]['PATTERN']
        print("Checking if {} matches {}".format(regex, data_file))
        match = re.search(regex, data_file)
        if match:
            release = job_types[type]['RELEASE']
            queue = job_types[type]['QUEUE']
            print("Data file '{}' matches job type info: "
                  "type: {}, release: {}, queue: {}".format(
                data_file, type, release, queue))
            return type, release, queue
    print("Could not match data file '{}' to a given job type: {}. "
          "Using default job type info".format(data_file, job_types.keys()))
    return default_type, default_release, default_queue


def submit_job(job_spec, job_params, queue, tags=[], priority=0):
    """Submit job to mozart via REST API."""

    # setup params
    params = {
        "queue": queue,
        "priority": priority,
        "tags": json.dumps(tags),
        "type": job_spec,
        "params": json.dumps(job_params),
        "name": "ingest-staged-{}".format(job_params['data_file']),
    }

    # submit job
    print("Job params: %s" % json.dumps(params))
    print("Job URL: %s" % JOB_SUBMIT_URL)
    req = requests.post(JOB_SUBMIT_URL, data=params, verify=False)

    print("Request code: %s" % req.status_code)
    print("Request text: %s" % req.text)
    print("Request Result: %s" % req.json())

    if req.status_code != 200:
        req.raise_for_status()
    result = req.json()
    
    if "result" in result.keys() and "success" in result.keys():
        if result["success"] is True:
            job_id = result["result"]
            print("submitted job: %s job_id: %s" % (job_spec, job_id))
        else:
            print("job not submitted successfully: %s" % result)
            raise Exception("job not submitted successfully: %s" % result)
    else:
        raise Exception("job not submitted successfully: %s" % result)


def lambda_handler(event, context):
    '''
    This lambda handler calls submit_job with the job type info
    and product id from the sns message
    '''

    print("Got event of type: %s" % type(event))
    print("Got event: %s" % json.dumps(event))
    print("Got context: %s"% context)
    print("os.environ: %s" % os.environ)
    # parse sns message
    message = json.loads(event["Records"][0]["Sns"]["Message"])
    print("Message : %s" % message)
    # parse s3 event
    s3_info = message['Records'][0]['s3']
    print("s3_info in message : %s " % s3_info)
    # parse signal and dataset files and urls
    bucket = s3_info['bucket']['name']
    #bucket = event['Records'][0]['s3']['bucket']['name']
    trigger_file = s3_info['object']['key']
    print("Trigger file: {}".format(trigger_file))
    if signal_file_suffix:
        ds_file = trigger_file.replace(signal_file_suffix, '')
    else:
        ds_file = trigger_file
    ds_url = "s3://%s/%s/%s" % (os.environ['DATASET_S3_ENDPOINT'], bucket,
                                ds_file)
    print("ds_url = {}".format(ds_url))
    # Create some metadata
    md = {
        "tags": ["ISL"],
        "ISL_urls": [ds_url],
        "SNS_record": event["Records"][0],
        "S3_event_record": message['Records'][0],
        "Lambda_trigger_time": datetime.utcnow().strftime(DATETIME_FORMAT)
    }
    print("Metadata created: {}".format(json.dumps(md, indent=2)))
    # data file
    id = data_file = os.path.basename(ds_url)
    
    # submit mozart jobs to update ES
    default_job_type = os.environ['JOB_TYPE'] # e.g. "INGEST_L0A_LR_RAW"
    default_job_release = os.environ['JOB_RELEASE'] # e.g. "gman-dev"
    default_queue = os.environ['JOB_QUEUE']
    job_types = {}
    if 'JOB_TYPES' in os.environ:
        job_types = json.loads(os.environ['JOB_TYPES'])

    job_type, job_release, queue = __get_job_type_info(data_file, job_types,
                                                       default_job_type,
                                                       default_job_release,
                                                       default_queue)

    job_spec = "job-%s:%s" % (job_type, job_release)
    job_params = {
        "id": id,
        "data_url": ds_url,
        "data_file": data_file,
        "prod_met": md,
    }
    tags = ["data-staged"]

    # submit mozart job
    submit_job(job_spec, job_params, queue, tags)
