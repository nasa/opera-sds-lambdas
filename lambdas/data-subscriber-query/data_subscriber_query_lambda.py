from __future__ import print_function

import json
import logging
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

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
JOB_NAME_DATETIME_FORMAT = "%Y%m%dT%H%M%S"

logger.info("Loading Lambda function")

if "MOZART_URL" not in os.environ:
    raise RuntimeError("Need to specify MOZART_URL in environment.")
MOZART_URL = os.environ["MOZART_URL"]
JOB_SUBMIT_URL = f"{MOZART_URL}/api/v0.1/job/submit?enable_dedup=false"


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
    logger.info(f"Job params: {json.dumps(params)}")
    logger.info(f"Job URL: {JOB_SUBMIT_URL}")
    req = requests.post(JOB_SUBMIT_URL, data=params, verify=False)

    logger.info(f"Request code: {req.status_code}")
    logger.info(f"Request text: {req.text}")

    req.raise_for_status()
    result = req.json()
    logger.info(f"Request Result: {result}")

    if "result" in result.keys() and "success" in result.keys():
        if result["success"] is True:
            job_id = result["result"]
            logger.info(f"submitted job: {job_spec} job_id: {job_id}")
            return job_id
        else:
            logger.info(f"job not submitted successfully: {result}")
            raise Exception(f"job not submitted successfully: {result}")
    else:
        raise Exception(f"job not submitted successfully: {result}")


def lambda_handler(event: Dict, context: LambdaContext):
    """
    This lambda handler calls submit_job with the job type info
    and dataset_type set in the environment
    """

    logger.info(f"Got event of type: {type(event)}")
    logger.info(f"Got event: {json.dumps(event)}")
    logger.info(f"Got context: {context}")
    logger.info(f"os.environ: {os.environ}")

    event = EventBridgeEvent(event)
    query_end_datetime = dateutil.parser.isoparse(event.time)

    minutes = re.search(r"\d+", os.environ["MINUTES"]).group()
    query_start_datetime = query_end_datetime + relativedelta(minutes=-int(minutes))

    provider = os.environ["PROVIDER"]

    job_type = os.environ["JOB_TYPE"]
    job_release = os.environ["JOB_RELEASE"]
    queue = os.environ["JOB_QUEUE"]
    job_spec = f"job-{job_type}:{job_release}"
    job_params = {
        "start_datetime": f"--start-date={query_start_datetime.strftime(DATETIME_FORMAT)}",
        "end_datetime": f"--end-date={query_end_datetime.strftime(DATETIME_FORMAT)}",
        "provider": f"-p {provider}",
        "endpoint": f'--endpoint={os.environ["ENDPOINT"]}',
        "bounding_box": "",
        "download_job_release": f'--release-version={os.environ["JOB_RELEASE"]}',
        "download_job_queue": f'--job-queue={os.environ["DOWNLOAD_JOB_QUEUE"]}',
        "chunk_size": f'--chunk-size={os.environ["CHUNK_SIZE"]}',
        "smoke_run": f'{"--smoke-run" if strtobool(os.environ["SMOKE_RUN"]) else ""}',
        "dry_run": f'{"--dry-run" if strtobool(os.environ["DRY_RUN"]) else ""}',
        "no_schedule_download": f'{"--no-schedule-download" if strtobool(os.environ["NO_SCHEDULE_DOWNLOAD"]) else ""}',
        "use_temporal": "",
        "temporal_start_datetime": os.environ.get("TEMPORAL_START_DATETIME", "")
    }
    
    tags = ["data-subscriber-query-timer"]
    job_name = f"data-subscriber-query-timer-{datetime.utcnow().strftime(JOB_NAME_DATETIME_FORMAT)}_{minutes}"
    # submit mozart job
    return submit_job(job_name, job_spec, job_params, queue, tags)
