import json
import os
import re
from typing import Dict


def strtobool(val):
    """Convert a string representation of truth to true (1) or false (0).

    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    val = val.lower()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return 1
    elif val in ('n', 'no', 'f', 'false', 'off', '0'):
        return 0
    else:
        raise ValueError(f"invalid truth value {val!r}")
import dateutil.parser
from pathlib import PurePath
import requests

import boto3

from types import SimpleNamespace
import time
from datetime import datetime, timedelta, timezone
from aws_lambda_powertools.utilities.data_classes import EventBridgeEvent
from aws_lambda_powertools.utilities.typing import LambdaContext
from hysds_commons.elasticsearch_utils import ElasticsearchUtility
import logging

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
JOB_NAME_DATETIME_FORMAT = "%Y%m%dT%H%M%S"

# Requires these 5 env variables
_ENV_MOZART_IP = "MOZART_IP"
_ENV_GRQ_IP = "GRQ_IP"
_ENV_GRQ_ES_PORT = "GRQ_ES_PORT"
_ENV_ENDPOINT = "ENDPOINT"
_ENV_JOB_RELEASE = "JOB_RELEASE"

for ev in [_ENV_MOZART_IP, _ENV_GRQ_IP, _ENV_ENDPOINT, _ENV_JOB_RELEASE, _ENV_GRQ_ES_PORT]:
    if ev not in os.environ:
        raise RuntimeError("Need to specify %s in environment." % ev)
MOZART_IP = os.environ[_ENV_MOZART_IP]
GRQ_IP = os.environ[_ENV_GRQ_IP]
GRQ_ES_PORT = os.environ[_ENV_GRQ_ES_PORT]
ENDPOINT = os.environ[_ENV_ENDPOINT]
JOB_RELEASE = os.environ[_ENV_JOB_RELEASE]

MOZART_URL = 'https://%s/mozart' % MOZART_IP
JOB_SUBMIT_URL = "%s/api/v0.1/job/submit?enable_dedup=false" % MOZART_URL

ES_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
ES_INDEX = 'batch_proc'
LOGGER = logging.getLogger(ES_INDEX)
eu = ElasticsearchUtility('http://%s:%s' % (GRQ_IP, str(GRQ_ES_PORT)), LOGGER)

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


def form_job_params(p, s_date, e_date):
    end_point = ENDPOINT
    download_job_queue = p.download_job_queue
    try:
        if p.temporal is True:
            temporal = True
        else:
            temporal = False
    except:
        print("Temporal parameter not found in batch proc. Defaulting to false.")
        temporal = False

    try:
        processing_mode = p.processing_mode
        if p.processing_mode == "historical":
            temporal = True  # temporal is always true for historical processing
    except:
        print("processing_mode parameter not found in batch proc. Defaulting to forward.")
        processing_mode = 'forward'

    job_spec = "job-%s:%s" % (p.job_type, JOB_RELEASE)
    job_params = {
        "start_datetime": f"--start-date={convert_datetime(s_date)}",
        "end_datetime": f"--end-date={convert_datetime(e_date)}",
        "endpoint": f'--endpoint={end_point}',
        "bounding_box": "",
        "download_job_release": f'--release-version={JOB_RELEASE}',
        "download_job_queue": f'--job-queue={download_job_queue}',
        "chunk_size": f'--chunk-size={p.chunk_size}',
        "processing_mode": f'--processing-mode={processing_mode}',
        "smoke_run": "",
        "dry_run": "",
        "no_schedule_download": "",
        "use_temporal": f'--use-temporal' if temporal is True else ''
    }

    # Include and exclude regions are optional
    try:
        includes = p.include_regions
        if len(includes.strip()) > 0:
            job_params["include_regions"] = f'--include-regions={includes}'
    except:
        pass
    try:
        excludes = p.exclude_regions
        if len(excludes.strip()) > 0:
            job_params["exclude_regions"] = f'--exclude-regions={excludes}'
    except:
        pass

    tags = ["data-subscriber-query-timer"]
    if processing_mode == 'historical':
        tags.append("historical_processing")
    else:
        tags.append("batch_processing")
    job_name = "data-subscriber-query-timer-{}_{}-{}".format(p.label, s_date.strftime(ES_DATETIME_FORMAT),
                                                             e_date.strftime(ES_DATETIME_FORMAT))

    return job_name, job_spec, job_params, tags

def form_tropo_job_params(p, s3_key, bucket_name, s_date, e_date):
    # Create the full S3 path
    s3_path = f"s3://{bucket_name}/{s3_key}"
    
    # Create the product structure with metadata
    product_metadata = {
        "dataset": f"L4_TROPO-{s3_key}",
        "metadata": {
            "batch_id": s3_key,
            "product_paths": {"L4_TROPO": [s3_path]},  # The S3 paths to localize
            "ProductReceivedTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "FileName": PurePath(s3_key).name,
            "FileLocation": s3_path,
            "id": s3_key,
            "Files": [
                {
                    "FileName": PurePath(s3_key).name,
                    "FileSize": 1,
                    "FileLocation": s3_path,
                    "id": PurePath(s3_key).name,
                    "product_paths": "$.product_paths"
                }
            ]
        }
    }

    # Create the job parameters
    params = {
        "dataset_type": "L4_TROPO",
        "input_dataset_id": s3_key,
        "product_metadata": product_metadata
    }

    p.label = p.label.replace(" ", "_")
    job_name = "job-WF-SCIFLO_L4-TROPO-{}-{}".format(p.label, PurePath(s3_key).name)
    job_spec = f"{p.job_type}:{JOB_RELEASE}"

    tags = ["l4-tropo-batch-timer"]
    if p.processing_mode == 'historical':
        tags.append("historical_processing")
    else:
        tags.append("batch_processing")
        
    return job_name, job_spec, params, tags

def get_tropo_input_prefixes(s_date, e_date):
    prefixes = set()
    current = s_date
    # Find the first 6-hour chunk that current intersects with
    if current.hour < 6:
        current = current.replace(hour=0)
    elif current.hour < 12:
        current = current.replace(hour=6) 
    elif current.hour < 18:
        current = current.replace(hour=12)
    else:
        current = current.replace(hour=18)
    current = current.replace(minute=0, second=0, microsecond=0)

    # Generate all 6-hour chunks between start and end dates
    # make sure the whole range ends before end time
    while current + timedelta(hours=6) <= e_date:
        prefixes.add(f'{current.strftime("%Y%m%d")}/ECMWF_TROP_{current.strftime("%Y%m%d%H00")}')
        current += timedelta(hours=6)
    
    print(prefixes)
    return prefixes

def submit_tropo_jobs(p, s_date, e_date): 
    """
    Generate a set of prefixes for all 6-hour chunks in the given range (inclusive).
    Each day is split into 4 chunks: 00:00, 06:00, 12:00, and 18:00.
    
    Args:
        start_datetime: Start datetime 
        end_datetime: End datetime 
        
    Returns:
        Set[str]: Set of prefix strings in YYYYmmddTHH0000 format
    """
    s3 = boto3.resource("s3") 
    bucket_name = p.s3_bucket_name
    bucket = s3.Bucket(bucket_name)

    prefixes = get_tropo_input_prefixes(s_date, e_date)

    # Form parameters for each job and submit them
    job_success = []
    for prefix in prefixes:
        for obj in bucket.objects.filter(Prefix=prefix):
            job_name, job_spec, job_params, tags = form_tropo_job_params(p, obj.key, bucket_name, s_date, e_date)
            job_success.append(submit_job(job_name, job_spec, job_params, p.job_queue, tags))

    # Return True if all jobs were successful, False otherwise
    return all(job_success)


def batch_proc_once():
    procs = eu.query(index=ES_INDEX)  # TODO: query for only enabled docs
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

        # tropo batch jobs
        if p.job_type == "job-SCIFLO_L4_TROPO":
            # Compute job parameters and submit job for tropo
            job_success = submit_tropo_jobs(p, s_date, e_date)
        # Non-tropo batch jobs
        else:
            # Compute job parameters
            (job_name, job_spec, job_params, job_tags) = form_job_params(p, s_date, e_date)
            # submit mozart job
            print("Submitting query job for", p.label, "with start date", s_date, "and end date", e_date)
            job_success = submit_job(job_name, job_spec, job_params, p.job_queue, job_tags)

        # Update last_successful_proc_data_date here
        eu.update_document(id=doc_id,
                           body={"doc_as_upsert": True,
                                 "doc": {
                                     "last_successful_proc_data_date": e_date, }},
                           index=ES_INDEX)

        return job_success


def lambda_handler(event: Dict, context: LambdaContext):
    """
    This lambda handler calls submit_job with the job type info
    and dataset_type set in the environment
    """

    event = EventBridgeEvent(event)

    print("Got event of type: %s" % type(event))
    # print("Got event: %s" % json.dumps(event))
    print("Got context: %s" % context)
    print("os.environ: %s" % os.environ)

    # submit mozart job
    return batch_proc_once()


if __name__ == '__main__':
    while (True):
        print(batch_proc_once())
        time.sleep(10)
