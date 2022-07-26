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
import base64
import backoff

print ('Loading function')

MOZART_URL = os.environ['MOZART_URL']
QUEUE = os.environ['JOB_QUEUE']
JOB_TYPE = os.environ['JOB_TYPE']
JOB_RELEASE = os.environ['JOB_RELEASE']
TAGGING = os.environ['PRODUCT_TAG']  # set to True if product in HySDS catalog should be tagged as delivered.
EVENT_TRIGGER = os.environ['EVENT_TRIGGER']

@backoff.on_exception(
    backoff.expo, requests.exceptions.RequestException, max_tries=8, max_value=32
)
def submit_job(job_type, release, product_id, tag, job_params, identifier=""):
    """
    submits a job to mozart
    :param job_type:
    :param release:
    :param product_id:
    :param tag:
    :param job_params:
    :return:
    """

    # submit mozart job
    print("submit_job : job_type : {}, release : {}, product_id : {}, tag : {}, job_params : {}, identifier : {}".format(job_type, release, product_id, tag, job_params, identifier))
    job_submit_url = '%s/api/v0.1/job/submit' % MOZART_URL
    params = {
        'queue': QUEUE,
        'priority': '5',
        'name': 'job_%s-%s' % ('process_cnm_response', identifier),
        'tags': json.dumps(tag),
        'type': 'job-%s:%s' % (job_type, release),
        'params': json.dumps(job_params),
        'enable_dedup': True

    }
    print ('submitting jobs with params:')
    print ('Job params: %s' % json.dumps(params, sort_keys=True, indent=4, 
                                         separators=(',', ': ')))
    print ('Job Submit URL: {}'.format(job_submit_url))
    req = requests.post(job_submit_url, data=params, verify=False)
    print("Request code: %s"% req.status_code)
    if req.status_code != 200:
        req.raise_for_status()
    result = req.json()
    print("Result: %s"% result)
    if 'result' in result.keys() and 'success' in result.keys():
        if result['success'] is True:
            job_id = result['result']
            print ('submitted upate ES:%s job: %s job_id: %s' % (job_type, release, job_id))
        else:
            print ('job not submitted successfully: %s' % result)
            raise Exception('job not submitted successfully: %s' % result)
    else:
        raise Exception('job not submitted successfully: %s' % result)


def lambda_handler(event, context):
    """
    This lambda handler calls submit_job with the job type info
    and product id from the sns message
    :param event:
    :param context:
    :return:
    """
    print ("Got event of type: %s" % type(event))
    print ("Got event: %s" % json.dumps(event, indent=2))
    print ("Got context: %s" % context)

    job_type = JOB_TYPE
    job_release = JOB_RELEASE
    job_tag = ["daac_response"]
    job_params = {}
    if TAGGING.lower() == 'true':
        job_params["product_tagging"] = True
    else:
        job_params["product_tagging"] = False

    # Explicitly set the update_s3_tag 
	# PCM to wait for CNM-R/SCNM-R success message before cleaning up OSL (https://github.jpl.nasa.gov/IEMS-SDS/CNM_product_delivery/pull/48)
    job_params["update_s3_tag"] = False

    event_trigger = EVENT_TRIGGER
    if event_trigger.lower() == "sns":
        cnm_message = json.loads(event["Records"][0]["Sns"]["Message"])
        print("Received message: {}".format(cnm_message))
        job_params["cnm_message"] = cnm_message
        product = cnm_message["collection"]
        identifier =  cnm_message.get("identifier", product)
        print("From CNM collection key: %s" % product)
        print("identifier : {}".format(identifier))
        submit_job(job_type, job_release, product, job_tag, job_params, identifier)
    elif event_trigger.lower() == "kinesis":
        # For Kinesis streams, we could be processing multiple messages
        # in a single trigger.
        for record in event['Records']:
            # Kinesis data is base64 encoded so decode here
            payload = base64.b64decode(record["kinesis"]["data"])
            print("Decoded payload: " + str(payload))
            cnm_message = json.loads(payload)
            print("Received message: {}".format(cnm_message))
            job_params["cnm_message"] = cnm_message
            product = cnm_message["collection"]
            identifier =  cnm_message.get("identifier", product)
            print("From CNM collection key: %s" % product)
            print("identifier : {}".format(identifier))
            submit_job(job_type, job_release, product, job_tag, job_params, identifier)
    elif event_trigger.lower() == "sqs":
        for event_record in event["Records"]:
            body = json.loads(event_record["body"])
            print("Body: {}".format(json.dumps(body, indent=2)))
            job_params["cnm_message"] = body
            product = body["collection"]
            identifier = body.get("identifier", product)
            print("CNM product: %s" % product)
            print("identifier : {}".format(identifier))
            submit_job(job_type, job_release, product, job_tag, job_params, identifier)
    else:
        raise RuntimeError(
            "EVENT_TRIGGER value not valid: {}. must be set to 'sns', 'sqs'"
            " or 'kinesis'".format(event_trigger))
