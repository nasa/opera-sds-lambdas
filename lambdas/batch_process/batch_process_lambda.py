from __future__ import print_function

import json
import os
import re
from datetime import datetime
from distutils.util import strtobool
from typing import Dict

import dateutil.parser
import requests
from aws_lambda_powertools.utilities.data_classes import EventBridgeEvent
from aws_lambda_powertools.utilities.typing import LambdaContext
from dateutil.relativedelta import relativedelta

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
JOB_NAME_DATETIME_FORMAT = "%Y%m%dT%H%M%S"

print("Loading Lambda function")

if "MOZART_URL" not in os.environ:
    raise RuntimeError("Need to specify MOZART_URL in environment.")
MOZART_URL = os.environ["MOZART_URL"]
JOB_SUBMIT_URL = "%s/api/v0.1/job/submit?enable_dedup=false" % MOZART_URL


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


def lambda_handler(event: Dict, context: LambdaContext):
    """
    This lambda handler calls submit_job with the job type info
    and dataset_type set in the environment
    """

    print("Got event of type: %s" % type(event))
    print("Got event: %s" % json.dumps(event))
    print("Got context: %s" % context)
    print("os.environ: %s" % os.environ)

    event = EventBridgeEvent(event)
    query_end_datetime = dateutil.parser.isoparse(event.time)

    minutes = re.search(r'\d+', os.environ['MINUTES']).group()
    query_start_datetime = query_end_datetime + relativedelta(minutes=-int(minutes))


    provider = os.environ['PROVIDER']

    job_type = os.environ['JOB_TYPE']
    job_release = os.environ['JOB_RELEASE']
    queue = os.environ['JOB_QUEUE']
    isl_bucket_name = os.environ['ISL_BUCKET_NAME']
    job_spec = "job-%s:%s" % (job_type, job_release)
    job_params = {
        "isl_bucket_name": f"--isl-bucket={isl_bucket_name}",
        "start_datetime": f"--start-date={convert_datetime(query_start_datetime)}",
        "end_datetime": f"--end-date={convert_datetime(query_end_datetime)}",
        "provider": f"-p {provider}",
        "endpoint": f'--endpoint={os.environ["ENDPOINT"]}',
        "bounding_box": "",
        "download_job_release": f'--release-version={os.environ["JOB_RELEASE"]}',
        "download_job_queue": f'--job-queue={os.environ["DOWNLOAD_JOB_QUEUE"]}',
        "chunk_size": f'--chunk-size={os.environ["CHUNK_SIZE"]}',
        "smoke_run": f'{"--smoke-run" if strtobool(os.environ["SMOKE_RUN"]) else ""}',
        "dry_run": f'{"--dry-run" if strtobool(os.environ["DRY_RUN"]) else ""}',
        "no_schedule_download": f'{"--no-schedule-download" if strtobool(os.environ["NO_SCHEDULE_DOWNLOAD"]) else ""}',
        "use_temporal": ""
    }
    
    tags = ["data-subscriber-query-timer"]
    job_name = "data-subscriber-query-timer-{}_{}".format(convert_datetime(datetime.utcnow(), JOB_NAME_DATETIME_FORMAT),
                                                          minutes)
    # submit mozart job
    return submit_job(job_name, job_spec, job_params, queue, tags)


from types import SimpleNamespace
import time
from datetime import datetime, timedelta, timezone
from hysds_commons.elasticsearch_utils import ElasticsearchUtility
import logging
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
ES_INDEX = 'batch_proc'

LOGGER = logging.getLogger(ES_INDEX)

eu = ElasticsearchUtility('http://100.104.40.166:9200', LOGGER)

while(True):
    procs = eu.query(index=ES_INDEX)
    for proc in procs:
        doc_id = proc['_id']
        proc = proc['_source']
        p = SimpleNamespace(**proc)

        # If this batch proc is disabled, continue
        if p.enabled == False:
            continue

        # If it's not time to run yet, just continue
        last_run_date = datetime.strptime(p.last_run_date, DATETIME_FORMAT)
        #TODO: update last_run_date here, note that the last_run_date we compare below still uses the old value
        if last_run_date + timedelta(minutes = p.run_interval_mins) > datetime.utcnow():
            continue

        data_start_date = datetime.strptime(p.data_start_date, DATETIME_FORMAT)
        data_end_date = datetime.strptime(p.data_end_date, DATETIME_FORMAT)

        s_date = datetime.strptime(p.last_successful_proc_data_date, DATETIME_FORMAT)
        if s_date < data_start_date:
            s_date = data_start_date

        e_date = s_date + timedelta(minutes=p.data_date_incr_mins)
        if e_date > data_end_date:
            e_date = data_end_date

        # See if we've reached the end of this batch proc. If so, disable it.
        if s_date >= data_end_date:
            #TODO: disable this proc
            break

        #TODO: update last_attempted_proc_data_date here


        print("Submitting query job with start date", s_date, "and end date",  e_date)
        #TODO: Submit the job

        #TODO: update last_successful_proc_data_date here
        eu.update_document(id=doc_id,
                            body={"doc_as_upsert": True,
                            "doc": {
                            "last_successful_proc_data_date": e_date, }},
                            index=ES_INDEX)

    time.sleep(10)

