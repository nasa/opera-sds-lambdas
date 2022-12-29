from __future__ import print_function
import json
import os
import re
from distutils.util import strtobool
from typing import Dict
import dateutil.parser
import requests

from types import SimpleNamespace
import time
from datetime import datetime, timedelta, timezone
from hysds_commons.elasticsearch_utils import ElasticsearchUtility
import logging

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
JOB_NAME_DATETIME_FORMAT = "%Y%m%dT%H%M%S"

# Requires these 4 env variables
_ENV_MOZART_IP = "MOZART_IP"
_ENV_GRQ_IP = "GRQ_IP"
_ENV_ENDPOINT = "ENDPOINT"
_ENV_JOB_RELEASE = "JOB_RELEASE"

for ev in [_ENV_MOZART_IP, _ENV_GRQ_IP, _ENV_ENDPOINT, _ENV_JOB_RELEASE]:
    if ev not in os.environ:
        raise RuntimeError("Need to specify %s in environment." % ev)
MOZART_IP = os.environ[_ENV_MOZART_IP]
GRQ_IP = os.environ[_ENV_GRQ_IP]
ENDPOINT = os.environ[_ENV_ENDPOINT]
JOB_RELEASE = os.environ[_ENV_JOB_RELEASE]

MOZART_URL = 'https://%s/mozart' % MOZART_IP
JOB_SUBMIT_URL = "%s/api/v0.1/job/submit?enable_dedup=false" % MOZART_URL

ES_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
ES_INDEX = 'batch_proc'
LOGGER = logging.getLogger(ES_INDEX)
eu = ElasticsearchUtility('http://%s:9200' % GRQ_IP, LOGGER)

print("Loading Lambda function")

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

def batch_proc_once():
    procs = eu.query(index=ES_INDEX) #TODO: query for only enabled docs
    for proc in procs:
        doc_id = proc['_id']
        proc = proc['_source']
        p = SimpleNamespace(**proc)

        # If this batch proc is disabled, continue TODO: this goes away when we change the query above
        if p.enabled == False:
            continue

        now = datetime.utcnow()
        new_last_run_date = datetime.strptime(p.last_run_date, ES_DATETIME_FORMAT) + timedelta(
            minutes=p.run_interval_mins)

        # If it's not time to run yet, just continue
        if new_last_run_date > now:
            continue

        # Update last_run_date here
        eu.update_document(id=doc_id,
                           body={"doc_as_upsert": True,
                                 "doc": {
                                     "last_run_date": now.strftime(ES_DATETIME_FORMAT), }},
                           index=ES_INDEX)

        data_start_date = datetime.strptime(p.data_start_date, ES_DATETIME_FORMAT)
        data_end_date = datetime.strptime(p.data_end_date, ES_DATETIME_FORMAT)

        # Start date time is when the last successful process data time.
        # If this is before the data start time, which may be the case when this batch_proc is first run,
        # change it to the data start time.
        s_date = datetime.strptime(p.last_successful_proc_data_date, ES_DATETIME_FORMAT)
        if s_date < data_start_date:
            s_date = data_start_date

        # End date time is when the start data time plus data increment time in minutes.
        # If this is after the data end time, which would be the case when this is the very last iteration of this proc,
        # change it to the data end time.
        e_date = s_date + timedelta(minutes=p.data_date_incr_mins)
        if e_date > data_end_date:
            e_date = data_end_date

        # See if we've reached the end of this batch proc. If so, disable it.
        if s_date >= data_end_date:
            print(p.label, "Batch Proc completed processing. It is now disabled")
            eu.update_document(id=doc_id,
                               body={"doc_as_upsert": True,
                                     "doc": {
                                         "enabled": False, }},
                               index=ES_INDEX)
            continue

        # update last_attempted_proc_data_date here
        eu.update_document(id=doc_id,
                           body={"doc_as_upsert": True,
                                 "doc": {
                                     "last_attempted_proc_data_date": e_date, }},
                           index=ES_INDEX)

        print("Submitting query job for", p.label, "with start date", s_date, "and end date", e_date)
        # Submit the job
        provider = p.provider_name
        job_type = p.job_type
        job_release = JOB_RELEASE
        queue = p.job_queue
        isl_bucket_name = p.ingest_s3
        end_point = ENDPOINT
        download_job_queue = p.download_job_queue
        job_spec = "job-%s:%s" % (job_type, job_release)
        job_params = {
            "isl_bucket_name": f"--isl-bucket={isl_bucket_name}",
            "start_datetime": f"--start-date={convert_datetime(s_date)}",
            "end_datetime": f"--end-date={convert_datetime(e_date)}",
            "provider": f"-p {provider}",
            "endpoint": f'--endpoint={end_point}',
            "bounding_box": "",
            "download_job_release": f'--release-version={job_release}',
            "download_job_queue": f'--job-queue={download_job_queue}',
            "chunk_size": f'--chunk-size={p.chunk_size}',
            "smoke_run": "",
            "dry_run": "",
            "no_schedule_download": "",
            "use_temporal": ""
        }

        tags = ["data-subscriber-query-timer"]
        job_name = "data-subscriber-query-timer-{}_{}-{}".format(p.label, s_date.strftime(ES_DATETIME_FORMAT),
                                                                 e_date.strftime(ES_DATETIME_FORMAT))
        # submit mozart job
        job_success = submit_job(job_name, job_spec, job_params, queue, tags)

        # Update last_successful_proc_data_date here
        eu.update_document(id=doc_id,
                           body={"doc_as_upsert": True,
                                 "doc": {
                                     "last_successful_proc_data_date": e_date, }},
                           index=ES_INDEX)

        return job_success

def lambda_handler(event: Dict, context):# LambdaContext): #TODO restore the param typing
    """
    This lambda handler calls submit_job with the job type info
    and dataset_type set in the environment
    """
    from aws_lambda_powertools.utilities.data_classes import EventBridgeEvent
    from aws_lambda_powertools.utilities.typing import LambdaContext
    event = EventBridgeEvent(event)

    print("Got event of type: %s" % type(event))
    print("Got event: %s" % json.dumps(event))
    print("Got context: %s" % context)
    print("os.environ: %s" % os.environ)

    # submit mozart job
    return batch_proc_once()

if __name__ == '__main__':
    while (True):
        print(batch_proc_once())
        time.sleep(10)