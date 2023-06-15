import json
import logging
import os
from datetime import datetime
from typing import Dict

import dateutil.parser
import requests
from aws_lambda_powertools.utilities.data_classes import EventBridgeEvent
from aws_lambda_powertools.utilities.typing import LambdaContext
from dateutil.relativedelta import relativedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DATETIME_ISO_8601_FORMAT = "%Y-%m-%dT%H:%M:%SZ"  # e.g. 12 AM January 1st, 1970 becomes "1970-01-01T00:00:00Z"
JOB_NAME_DATETIME_FORMAT = "%Y%m%dT%H%M%S"  # e.g. 12 AM January 1st, 1970 becomes "19700101T000000"

MOZART_URL = os.environ['MOZART_URL']
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


def _create_job(event: Dict):
    event = EventBridgeEvent(event)

    # NOTE: ionosphere correction files may not be available for up to 30 hours after SLC product availability
    #  Set offsets accordingly.

    query_end_datetime_offset_hours = int(os.environ["QUERY_END_DATETIME_OFFSET_HOURS"])
    query_end_datetime = dateutil.parser.isoparse(event.time) - relativedelta(hours=query_end_datetime_offset_hours)

    query_start_datetime_offset_hours = int(os.environ["QUERY_START_DATETIME_OFFSET_HOURS"])
    query_start_datetime = query_end_datetime - relativedelta(hours=query_start_datetime_offset_hours)

    job_type = os.environ["JOB_TYPE"]
    job_release = os.environ["JOB_RELEASE"]
    job_params = {
        "start_datetime": f"--start-date={query_start_datetime.strftime(DATETIME_ISO_8601_FORMAT)}",
        "end_datetime": f"--end-date={query_end_datetime.strftime(DATETIME_ISO_8601_FORMAT)}",
        "cslc_job_release": f'--release-version={job_release}'
    }

    tags = ["data-subscriber-download-slc-ionosphere-timer"]
    job_name = f"data-subscriber-download-slc-ionosphere-timer-{datetime.utcnow().strftime(JOB_NAME_DATETIME_FORMAT)}_{query_start_datetime_offset_hours}"

    job_spec = f"job-{job_type}:{job_release}"
    queue = os.environ["JOB_QUEUE"]
    return job_name, job_spec, job_params, queue, tags


def lambda_handler(event: Dict, context: LambdaContext):
    """
    This lambda handler calls submit_job with the job type info
    and dataset_type set in the environment
    """

    logger.info(f"Got event of type: {type(event)}")
    logger.info(f"Got event: {json.dumps(event)}")
    logger.info(f"Got context: {context}")
    logger.info(f"os.environ: {os.environ}")

    job_name, job_spec, job_params, queue, tags = _create_job(event)

    # submit mozart job
    return submit_job(job_name, job_spec, job_params, queue, tags)
