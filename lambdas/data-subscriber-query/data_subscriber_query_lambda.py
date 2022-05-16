from __future__ import print_function

import json
import os
import re
from datetime import datetime

import requests

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
JOB_NAME_DATETIME_FORMAT = "%Y%m%dT%H%M%S"

print("Loading Lambda function")

if "MOZART_URL" not in os.environ:
    raise RuntimeError("Need to specify MOZART_URL in environment.")
MOZART_URL = os.environ["MOZART_URL"]
JOB_SUBMIT_URL = "%s/api/v0.1/job/submit" % MOZART_URL


def convert_datetime(datetime_obj, strformat=DATETIME_FORMAT):
    """
    Converts from a datetime string to a datetime object or vice versa
    """
    if isinstance(datetime_obj, datetime):
        return datetime_obj.strftime(strformat)
    return datetime.strptime(str(datetime_obj), strformat)


def submit_job(job_name, job_spec, job_params, queue, tags, priority=0):
    """Submit job to mozart via REST API."""

    # setup params
    params = {
        "queue": queue,
        "priority": priority,
        "tags": json.dumps(tags),
        "type": job_spec,
        "params": json.dumps(job_params),
        "name": job_name,
    }

    # submit job
    print("Job params: %s" % json.dumps(params))
    print("Job URL: %s" % JOB_SUBMIT_URL)
    req = requests.post(JOB_SUBMIT_URL, data=params, verify=False)

    print("Request code: %s" % req.status_code)
    print("Request text: %s" % req.text)

    if req.status_code != 200:
        req.raise_for_status()
    result = req.json()
    print("Request Result: %s" % result)

    if "result" in result.keys() and "success" in result.keys():
        if result["success"] is True:
            job_id = result["result"]
            print("submitted job: %s job_id: %s" % (job_spec, job_id))
            return job_id
        else:
            print("job not submitted successfully: %s" % result)
            raise Exception("job not submitted successfully: %s" % result)
    else:
        raise Exception("job not submitted successfully: %s" % result)


def lambda_handler(event, context):
    """
    This lambda handler calls submit_job with the job type info
    and dataset_type set in the environment
    """

    print("Got event of type: %s" % type(event))
    print("Got event: %s" % json.dumps(event))
    print("Got context: %s" % context)
    print("os.environ: %s" % os.environ)

    minutes = re.search(r'\d+', os.environ['MINUTES']).group()

    job_type = os.environ['JOB_TYPE']
    job_release = os.environ['JOB_RELEASE']
    queue = os.environ['JOB_QUEUE']
    job_spec = "job-%s:%s" % (job_type, job_release)
    job_params = {
        "minutes": minutes,
        "download_job_release": os.environ["JOB_RELEASE"],
        "download_job_queue": os.environ["DOWNLOAD_JOB_QUEUE"],
        "chunk_size": os.environ["CHUNK_SIZE"],
        "smoke_run": os.environ["SMOKE_RUN"],
        "dry_run": os.environ["DRY_RUN"]
    }
    tags = ["data-subscriber-query-timer"]
    job_name = "data-subscriber-query-timer-{}_{}".format(convert_datetime(datetime.utcnow(), JOB_NAME_DATETIME_FORMAT),
                                                          minutes)
    # submit mozart job
    return submit_job(job_name, job_spec, job_params, queue, tags)
