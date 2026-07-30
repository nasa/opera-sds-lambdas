#!/usr/bin/env python
from __future__ import print_function

'''
This lambda function submits a job via MOZART API
to update ES doc, for book keeping purposes.
When an SNS message is recieved reporting
delivery of a product to our archive bucket,
we want to capture this acknowledgement by stamping the product
with delivery and ingestion time.
'''

import os
import json
import requests
import boto3
from datetime import datetime

print('Loading function')

MOZART_ES_URL = os.environ['MOZART_ES_URL']
CLOUDWATCH_METRIC_NAME = os.environ['CLOUDWATCH_METRIC_NAME']
CLOUDWATCH_METRIC_NAMESPACE = os.environ['CLOUDWATCH_METRIC_NAMESPACE']
CLUSTER_NAME = os.environ['CLUSTER_NAME']

def lambda_handler(event, context):
    """
    This lambda handler calls submit_job with the job type info
    and product id from the sns message
    """
    print("Got event of type: %s" % type(event))
    print("Got event: %s" % json.dumps(event))
    print("Got context type: %s" % type(context))

    # Get the number of shards currently allocated on each node
    try:
        req = requests.get(f"{MOZART_ES_URL}/_cat/allocation?v&s=node&format=json", verify=False)
        print("Request code: %s" % req.status_code)
        print("Request text: %s" % req.text)

        if req.status_code != 200:
            req.raise_for_status()
        results = req.json()
        print("Current shard request result: %s" % results)
        if len(results) != 0:
            num_nodes = 0
            total_shards = 0
            for result in results:
                # ignore results from an UNASSIGNED node
                if result.get("node", "UNASSIGNED") == "UNASSIGNED":
                    continue
                current_shards = int(result.get('shards', -1))
                if current_shards == -1:
                    raise ValueError(f"Could not find the current number of open shards from the response: {result}")
                total_shards += current_shards
                num_nodes += 1
            average_shards = total_shards / num_nodes
            print(f"Average shards across all nodes: {average_shards}, nodes={num_nodes}")
            max_shards_req = requests.get(f"{MOZART_ES_URL}/_cluster/settings", verify=False)
            if max_shards_req.status_code != 200:
                max_shards_req.raise_for_status()
            max_shard_result = max_shards_req.json()
            print("Max shard request result: %s" % max_shard_result)
            max_shards = int(max_shard_result.get("persistent", {}).get("cluster", {}).get("max_shards_per_node", -1))
            if max_shards == -1:
                raise ValueError(f"Could not find the max number of shards from the response: {max_shard_result}")
            shard_usage = (average_shards / max_shards) * 100.0
            print("shard usage: %0.2f" % shard_usage)
            # Now stream to cloudwatch metrics
            cw_client = boto3.client("cloudwatch")

            metrics_data = {
                'Namespace': CLOUDWATCH_METRIC_NAMESPACE,
                'MetricData': [
                    {
                        "MetricName": CLOUDWATCH_METRIC_NAME,
                        "Value": shard_usage,
                        "Unit": "Percent",
                        "Dimensions": [
                            {
                                "Name": "Cluster",
                                "Value": CLUSTER_NAME
                            }
                        ]
                    }
                ]
            }

            print(f'Metrics data: {metrics_data}')
            response = cw_client.put_metric_data(**metrics_data)
            print(f"Response from publishing cloudwatch metric: {response}")

            return {
                'MetricsData': metrics_data,
                'CloudWatchResponse': response
            }
        else:
            print(f"Cannot determine shards from request response: {results}")
    except Exception as e:
        raise Exception(f"Error occurred while trying to query OpenSearch: {str(e)}")
