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

def _create_job(event: Dict):
    event = EventBridgeEvent(event)

    query_end_datetime = dateutil.parser.isoparse(event.time)

    # Offset the revision start and stop time if specified
    try:
        revision_offset_mins = os.environ["REVISION_START_DATETIME_MARGIN_MINS"]
        query_end_datetime = query_end_datetime - relativedelta(minutes=int(revision_offset_mins))
        logger.info(f"Using REVISION_START_DATETIME_MARGIN_MINS={revision_offset_mins}")
    except Exception:
        logger.warning(
            "Exception while parsing REVISION_START_DATETIME_MARGIN_MINS. Using default value of 0. Ignore if this was intentional.")

    # Get OS environment variable k and m if they exist
    cslc_processing_k = None
    cslc_processing_m = None
    try:
        cslc_processing_k = os.environ["CSLC_PROCESSING_K"]
        logger.info(f"Using K={cslc_processing_k}")
        cslc_processing_m = os.environ["CSLC_PROCESSING_M"]
        logger.info(f"Using M={cslc_processing_m}")
    except Exception:
        pass

    # Get OS environment variable GRACE_MINS if it exists
    grace_mins = None
    try:
        grace_mins = os.environ["GRACE_MINS"]
        logger.info(f"Using GRACE_MINS={grace_mins}")
    except Exception:
        pass

    # Get OS environment variable COVERAGE_PERCENTAGE if it exists
    coverage_percentage = None
    try:
        coverage_percentage = os.environ["COVERAGE_PERCENTAGE"]
        logger.info(f"Using COVERAGE_PERCENTAGE={coverage_percentage}")
    except Exception:
        pass

    # Get OS environment variable COVERAGE_NUM if it exists
    coverage_num = os.environ.get("COVERAGE_NUM")
    logger.info(f"Using COVERAGE_NUM={coverage_num}")

    minutes = re.search(r"\d+", os.environ["MINUTES"]).group()
    query_start_datetime = query_end_datetime - relativedelta(minutes=int(minutes))

    temporal_start_datetime = get_temporal_start_datetime(query_end_datetime)

    bounding_box = os.environ.get("BOUNDING_BOX")

    job_type = os.environ["JOB_TYPE"]
    job_release = os.environ["JOB_RELEASE"]
    queue = os.environ["JOB_QUEUE"]
    job_spec = f"job-{job_type}:{job_release}"
    job_params = {
        "start_datetime": f"--start-date={query_start_datetime.strftime(DATETIME_FORMAT)}",
        "end_datetime": f"--end-date={query_end_datetime.strftime(DATETIME_FORMAT)}",
        "endpoint": f'--endpoint={os.environ["ENDPOINT"]}',
        "download_job_release": f'--release-version={os.environ["JOB_RELEASE"]}',
        "download_job_queue": f'--job-queue={os.environ["DOWNLOAD_JOB_QUEUE"]}',
        "chunk_size": f'--chunk-size={os.environ["CHUNK_SIZE"]}',
        "max_revision": f'--max-revision={os.environ["MAX_REVISION"]}',
        "k": f"--k={cslc_processing_k}" if cslc_processing_k else "",
        "m": f"--m={cslc_processing_m}" if cslc_processing_m else "",
        "grace_mins": f"--grace-mins={grace_mins}" if grace_mins else "",
        "coverage_percentage": f"--coverage-percentage={coverage_percentage}" if coverage_percentage else "",
        "coverage_num": f"--coverage-num={coverage_num}" if coverage_num else "",
        "smoke_run": f'{"--smoke-run" if strtobool(os.environ["SMOKE_RUN"]) else ""}',
        "dry_run": f'{"--dry-run" if strtobool(os.environ["DRY_RUN"]) else ""}',
        "no_schedule_download": f'{"--no-schedule-download" if strtobool(os.environ["NO_SCHEDULE_DOWNLOAD"]) else ""}',
        "use_temporal": f'{"--use-temporal" if strtobool(os.environ["USE_TEMPORAL"]) else ""}',
        "temporal_start_datetime": f'--temporal-start-date={temporal_start_datetime}' if temporal_start_datetime else "",
        "bounding_box": f'--bounds={bounding_box}' if bounding_box else ""
    }

    tags = ["data-subscriber-query-timer"]
    job_name = f"data-subscriber-query-timer-{datetime.utcnow().strftime(JOB_NAME_DATETIME_FORMAT)}_{minutes}"

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


def get_temporal_start_datetime(query_end_datetime):
    try:
        temporal_start_datetime_margin_days = os.environ.get("TEMPORAL_START_DATETIME_MARGIN_DAYS", "")
        temporal_start_datetime = (query_end_datetime - relativedelta(days=int(temporal_start_datetime_margin_days))).strftime(DATETIME_FORMAT)
        logger.info(f"Using TEMPORAL_START_DATETIME_MARGIN_DAYS={temporal_start_datetime_margin_days}")
    except Exception:
        logger.warning("Exception while parsing TEMPORAL_START_DATETIME_MARGIN_DAYS. Falling back to TEMPORAL_START_DATETIME. Ignore if this was intentional.")

        temporal_start_datetime = os.environ.get("TEMPORAL_START_DATETIME", "")
        logger.info(f"Using TEMPORAL_START_DATETIME={temporal_start_datetime}")

    logger.info(f'{temporal_start_datetime=}')
    return temporal_start_datetime
