import argparse
import json
import logging
import os
from datetime import datetime, timezone

import dateutil.parser
import requests
from dateutil.relativedelta import relativedelta


logger = logging.getLogger()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
)
logger.setLevel(logging.INFO)

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
JOB_NAME_DATETIME_FORMAT = "%Y%m%dT%H%M%S"


def submit_job(
        mozart_host,
        job_type,
        job_release,
        job_queue,
        priority,
        tags,
        query_end_dt: datetime,
        # use_temporal,
        minutes,
        revision_margin,
        dedup,
        **extra_params
):
    mozart_url = f'https://{mozart_host}/mozart/api/v0.1/job/submit'

    job_params: dict = {}

    query_end_dt = query_end_dt - relativedelta(minutes=revision_margin)
    query_start_dt = query_end_dt - relativedelta(minutes=minutes)

    # *_PARAM = param name in HySDS IO, *_PARAM_PREFIX useful if param values are CLI parameters (eg. --start-date=)
    job_params[os.getenv('START_PARAM', 'start_date')] = f'{os.getenv('START_PARAM_PREFIX', '')}{query_start_dt.strftime(DATETIME_FORMAT)}'
    job_params[os.getenv('END_PARAM', 'end_date')] = f'{os.getenv('END_PARAM_PREFIX', '')}{query_end_dt.strftime(DATETIME_FORMAT)}'

    job_params.update(extra_params)

    payload = {
        'queue': job_queue,
        "priority": priority,
        'tags': tags,
        'type': f'job-{job_type}:{job_release}',
        'params': json.dumps(job_params),
        'name': f'catalog-ingest-timer-{job_type}-{datetime.now(timezone.utc).replace(tzinfo=None).strftime(JOB_NAME_DATETIME_FORMAT)}_{minutes}',
        'enable_dedup': dedup
    }

    logger.info(f'Mozart payload: {json.dumps(payload)}')
    logger.info(f'Submitting job to {mozart_url}')

    resp = requests.post(mozart_url, json=payload, headers={'Content-Type': 'application/json'}, verify=False)

    logger.info(f"Request code: {resp.status_code}")
    logger.info(f"Request text: {resp.text}")
    resp.raise_for_status()

    result = resp.json()

    if "result" in result.keys() and "success" in result.keys():
        if result["success"] is True:
            job_id = result["result"]
            logger.info(f"submitted job: {job_type} job_id: {job_id}")
            return job_id
        else:
            logger.error(f"job not submitted successfully: {result}")
            raise Exception(f"job not submitted successfully: {result}")
    else:
        raise Exception(f"job not submitted successfully: {result}")


def lambda_handler(event, context):
    logger.info(f"Got event: {json.dumps(event)}")

    mozart_host = os.environ['MOZART_HOST']
    job_release = os.environ['JOB_RELEASE']

    job_type = event.pop('job_type')
    job_queue = event.pop('job_queue')
    priority = event.pop('priority')
    tags = event.pop('tags')
    minutes = event.pop('minutes', 60)
    revision_margin = event.pop('revision_margin', 0)
    extra_params = event.pop('extra_params', {})
    dedup = event.pop('enable_dedup', False)

    event_time = event.pop('time')

    submit_job(
        mozart_host,
        job_type,
        job_release,
        job_queue,
        priority,
        tags,
        dateutil.parser.isoparse(event_time),
        minutes,
        revision_margin,
        dedup,
        **extra_params
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('mozart_host', help='Mozart host (IP or hostname with optional port)')
    parser.add_argument('job_type', help='Type of job to submit')
    parser.add_argument('job_release', help='Job release')
    parser.add_argument('job_queue', help='Job queue')
    parser.add_argument('priority', type=int, help='Job priority')
    parser.add_argument('tags', help='Tags')
    parser.add_argument('minutes', type=int, help='minutes')
    parser.add_argument('revision_margin', type=int, help='revision_margin')
    parser.add_argument('--dedup', action='store_true', help='Enable job dedupe')

    args = parser.parse_args()

    submit_job(
        args.mozart_host,
        args.job_type,
        args.job_release,
        args.job_queue,
        args.priority,
        args.tags,
        datetime.now(timezone.utc),
        args.minutes,
        args.revision_margin,
        args.dedup,
    )
