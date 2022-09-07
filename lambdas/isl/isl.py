from __future__ import print_function

import os, sys, re, json, requests, boto3, base64
from datetime import datetime

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

print("Loading ISL Lambda function")

signal_file_suffix = None

if "SIGNAL_FILE_SUFFIX" in os.environ:
    signal_file_suffix = json.loads(os.environ["SIGNAL_FILE_SUFFIX"])

if "MOZART_URL" not in os.environ:
    raise RuntimeError("Need to specify MOZART_URL in environment.")

MOZART_URL = os.environ["MOZART_URL"]
JOB_SUBMIT_URL = "%s/api/v0.1/job/submit" % MOZART_URL


def __get_job_type_info(
    data_file, job_types, default_type, default_release, default_queue
):
    """
    Determine the job type.
    :param data_file: The data file being ingested.
    :param job_types: A mapping of job types to a regex.
    :param default_type: Default job type.
    :param default_release: Default job release version.
    :param: default_queue: Default job queue.
    :return: Function will try to match the given data file to one of 
    the types specified in the job type mapping. If no mapping exists 
    or a match could not be found, then it will use the default job
    type, release, and queue.
    """
    for type in job_types.keys():
        regex = job_types[type]["PATTERN"]
        print("Checking if {} matches {}".format(regex, data_file))
        match = re.search(regex, data_file)
        if match:
            release = job_types[type]["RELEASE"]
            queue = job_types[type]["QUEUE"]
            print(
                "Data file '{}' matches job type info: "
                "type: {}, release: {}, queue: {}".format(
                    data_file, type, release, queue
                )
            )
            return type, release, queue
    print(
        "Could not match data file '{}' to a given job type: {}. "
        "Using default job type info".format(data_file, job_types.keys())
    )
    return default_type, default_release, default_queue


def submit_job(job_spec, job_params, queue, tags=[], priority=0):
    """Submit job to mozart via REST API."""

    # setup params
    params = {
        "queue": queue,
        "priority": priority,
        "tags": json.dumps(tags),
        "type": job_spec,
        "params": json.dumps(job_params),
        "name": "ingest-staged-{}".format(job_params["data_file"]),
    }

    # submit job
    print("Job params: %s" % json.dumps(params))
    print("Job URL: %s" % JOB_SUBMIT_URL)
    req = requests.post(JOB_SUBMIT_URL, data=params, verify=False)

    print("Request code: %s" % req.status_code)
    print("Request text: %s" % req.text)
    print("Request Result: %s" % req.json())

    if req.status_code != 200:
        req.raise_for_status()
    result = req.json()

    if "result" in result.keys() and "success" in result.keys():
        if result["success"] is True:
            job_id = result["result"]
            print("submitted job: %s job_id: %s" % (job_spec, job_id))
        else:
            print("job not submitted successfully: %s" % result)
            raise Exception("job not submitted successfully: %s" % result)
    else:
        raise Exception("job not submitted successfully: %s" % result)


def delete_isl_messages(event, entries):
    sqs_isl_arn = event["Records"][0]["eventSourceARN"]
    sqs_isl_url_part_1 = ".".join(sqs_isl_arn.split(":")[2:4])
    sqs_isl_url_part_2 = "/".join(sqs_isl_arn.split(":")[4:])
    sqs_isl_url = (
        "https://" + sqs_isl_url_part_1 + ".amazonaws.com/" + sqs_isl_url_part_2
    )

    sqs_client = boto3.client("sqs")
    response = sqs_client.delete_message_batch(QueueUrl=sqs_isl_url, Entries=entries)

    if "Successful" in response:
        for deleted in response["Successful"]:
            print("Successfully Deleted: {}".format(deleted["Id"]))

    if "Failed" in response:
        for failed in response["Failed"]:
            print("Failed to Delete: \n{}".format(json.dumps(failed)))

def send_SNS_message(message):
    client = boto3.client("sns")
    response = client.publish(TargetArn=os.environ["ISL_SNS_TOPIC"], Message=message,)


def parse_signal_file(bucket, filename):
    s3 = boto3.resource("s3")
    obj = s3.Object(bucket, filename)
    body = obj.get()["Body"].read().decode("utf-8").splitlines()
    print("Signal File Body = {}".format(body))
    arr = []
    for line in body:
        # remove empty line
        if line:
            arr.append(line)
    return arr


def get_group(file_name):
    group = file_name.split("_")[8][1:]
    return group


def lambda_handler(event, context):
    """
    This lambda handler calls submit_job with the job type info
    and product id from the sqs message
    """
    print("Got event of type: %s" % type(event))
    print("Got event: %s" % json.dumps(event))
    print("Got context: %s" % context)
    print("os.environ: %s" % os.environ)
    entries = []
    for record in event["Records"]:
        is_urgent_response = False
        checksum = False
        checksum_type = None
        signal_ds_url = None
        entries.append(
            {"Id": record["messageId"], "ReceiptHandle": record["receiptHandle"]}
        )
        # parse sqs message
        message = json.loads(record["body"])
        print("Message : %s" % message)
        # parse s3 event
        s3_info = message["Records"][0]["s3"]
        print("s3_info in message : %s " % s3_info)
        # parse signal and dataset files and urls
        bucket = s3_info["bucket"]["name"]
        # bucket = event['Records'][0]['s3']['bucket']['name']
        trigger_file = s3_info["object"]["key"]
        # trigger_file has the prefix to know what kind of file is being ingested.
        file_type = trigger_file[: trigger_file.find("/")]
        print("Trigger file: {}".format(trigger_file))
        s3obj_etag = s3_info["object"]["eTag"]
        print("S3 eTag: {}".format(s3obj_etag))

        s3 = boto3.resource("s3")
        metreq = os.environ["MET_REQUIRED"]
        gds_obj = s3.Object(bucket, trigger_file)

        if signal_file_suffix.get(file_type) is None:
            # this file type doesn't have an associated signal file
            ds_file = trigger_file
            if file_type == "tlm":
                if get_group(trigger_file) == "01":
                    is_urgent_response = True
                client = boto3.client("s3")
                res = client.head_object(Bucket=bucket, Key=ds_file)
                checksum = res["Metadata"]["md5checksum"]
                checksum_type = "md5"
        else:
            # this file type has a signal file
            # set signal file url
            signal_ds_url = "s3://%s/%s/%s" % (
                os.environ["DATASET_S3_ENDPOINT"],
                bucket,
                trigger_file,
            )
            # handling .signal file in met_required/ directory
            if file_type == metreq:
                signal = signal_file_suffix[file_type]["ext"]
                ds_file = trigger_file.replace(signal, "")
                if trigger_file.endswith(signal):
                    print("has suffix {}".format(signal))
                # not signal file suffix, so skip
                else:
                    # don't submit ingest job if not triggered by signal file.
                    print(
                        "Lambda triggered by non-signal file {}. Aborting ingest job submission".format(
                            trigger_file
                        )
                    )
                    continue

        if file_type != metreq:
            ds_url = [
                "s3://%s/%s/%s" % (os.environ["DATASET_S3_ENDPOINT"], bucket, ds_file)
            ]
            if signal_ds_url is not None:
                ds_url.append(signal_ds_url)
        else:
            file_list = parse_signal_file(bucket, trigger_file)
            # ds_url = ["s3://%s/%s/%s" % (os.environ["DATASET_S3_ENDPOINT"], bucket, ds_file)]
            ds_url = []
            isl_url = []
            for f in file_list:
                signal_ds_url = "s3://%s/%s/%s/%s" % (
                    os.environ["DATASET_S3_ENDPOINT"],
                    bucket,
                    file_type,
                    f,
                )
                ds_url.append(signal_ds_url)
                isl_url.append(signal_ds_url)
            # signal file
            signal_file_url = "s3://%s/%s/%s" % (
                os.environ["DATASET_S3_ENDPOINT"],
                bucket,
                trigger_file,
            )
            # add signal file to isl_url so it can be purged by the purge isl job
            isl_url.append(signal_file_url)

        print("ds_url = {}".format(json.dumps(ds_url)))

        # Create some metadata
        md = {
            "tags": ["ISL"],
            "ISL_urls": isl_url if file_type == metreq else ds_url,
            "restaged": True if file_type == metreq else False,
            "SQS_record": event["Records"][0],
            "S3_event_record": message["Records"][0],
            "Lambda_trigger_time": datetime.utcnow().strftime(DATETIME_FORMAT),
        }
        print("Metadata created: {}".format(json.dumps(md, indent=2)))

        # data file
        id = data_file = os.path.basename(ds_url[0])

        # submit mozart jobs to update ES
        default_job_type = os.environ["JOB_TYPE"]  # e.g. "INGEST_L0A_LR_RAW"
        default_job_release = os.environ["JOB_RELEASE"]  # e.g. "gman-dev"
        default_queue = os.environ["JOB_QUEUE"]
        job_types = {}
        if "JOB_TYPES" in os.environ:
            job_types = json.loads(os.environ["JOB_TYPES"])

        job_type, job_release, queue = __get_job_type_info(
            data_file, job_types, default_job_type, default_job_release, default_queue,
        )

        job_spec = "job-%s:%s" % (job_type, job_release)
        job_params = {
            "id": id,
            "data_url": ds_url,
            "data_file": data_file,
            "prod_met": md,
            "checksum": checksum,
            "checksum_type": checksum_type,
            "payload_hash": s3obj_etag,
        }
        tags = ["data-staged"]

        # submit mozart job
        print("Job Params: {}".format(json.dumps(job_params)))
        if is_urgent_response:
            print("Urgent Job Params: {}".format(json.dumps(job_params)))
            submit_job(job_spec, job_params, queue, tags, 5)
        else:
            submit_job(job_spec, job_params, queue, tags)
    delete_isl_messages(event, entries)
