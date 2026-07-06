import argparse
import json
import logging
import os

import requests


logger = logging.getLogger()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
)
logger.setLevel(logging.INFO)


def grq_on_demand(
        mozart_host,
        query,
        job_type,
        job_release,
        job_queue,
        priority,
        tags,
        kwargs,
        dedup,
        **payload_kwargs
):
    grq_url = f'https://{mozart_host}/grq/api/v0.1/grq/on-demand'

    payload = {
        'tags': tags,
        'job_type': f'hysds-io-{job_type}:{job_release}',
        'hysds_io': f'hysds-io-{job_type}:{job_release}',
        'queue': job_queue,
        'priority': priority,
        'query': json.dumps(query) if isinstance(query, (dict, list)) else query,
        'kwargs': json.dumps(kwargs) if isinstance(kwargs, (dict, list)) else kwargs,
        'enable_dedup': dedup,
    }

    payload.update(payload_kwargs)

    logger.info(f'GRQ payload: {json.dumps(payload)}')
    logger.info(f'Submitting on-demand job to {grq_url}')

    resp = requests.post(grq_url, json=payload, headers={'Content-Type': 'application/json'}, verify=False)

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

    es_query = event.pop('es_query')
    job_type = event.pop('job_type')
    job_queue = event.pop('job_queue')
    priority = event.pop('priority')
    tags = event.pop('tags')
    kwargs = event.pop('kwargs')
    dedup = event.pop('enable_dedup')

    grq_on_demand(
        mozart_host,
        es_query,
        job_type,
        job_release,
        job_queue,
        priority,
        tags,
        kwargs,
        dedup,
        **event
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('mozart_host', help='Mozart host (IP or hostname with optional port)')
    parser.add_argument('es_query', help='Path to file with es query')
    parser.add_argument('job_type', help='Type of job to submit')
    parser.add_argument('job_release', help='Job release')
    parser.add_argument('job_queue', help='Job queue')
    parser.add_argument('priority', type=int, help='Job priority')
    parser.add_argument('tags', help='Tags')
    parser.add_argument('--dedup', action='store_true', help='Enable job dedupe')
    parser.add_argument('--kwargs', required=False, help='Path to JSON file with job kwargs')

    args = parser.parse_args()

    with open(args.es_query) as fp:
        es_query = json.load(fp)

    if args.kwargs is not None:
        with open(args.kwargs) as fp:
            kwargs = json.load(fp)
    else:
        kwargs = {}

    grq_on_demand(
        args.mozart_host,
        es_query,
        args.job_type,
        args.job_release,
        args.job_queue,
        args.priority,
        args.tags,
        kwargs,
        args.dedup,
    )
