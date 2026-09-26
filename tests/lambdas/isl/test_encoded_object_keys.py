"""Exercise ISL SQS/SNS handlers with form-encoded S3 notification keys."""

import importlib.util
import json
import os
from copy import deepcopy
from io import BytesIO
from pathlib import Path
from urllib.parse import quote_plus

import pytest
import requests

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def load_handler(monkeypatch, mocker):
    def load(transport, suffix=None):
        # Use only synthetic configuration; the handlers log the environment.
        environment = {
            "MOZART_URL": "https://example.invalid/mozart",
            "DATASET_S3_ENDPOINT": "s3-us-west-2.amazonaws.com",
            "JOB_TYPE": "default-ingest",
            "JOB_RELEASE": "test-release",
            "JOB_QUEUE": "test-queue",
            "MET_REQUIRED": "met_required",
            "SIGNAL_FILE_SUFFIX": json.dumps(suffix or {})
            if transport == "sqs"
            else (suffix or ""),
        }
        monkeypatch.setattr(os, "environ", environment)
        script = "isl/isl.py" if transport == "sqs" else "isl-sns/isl-sns.py"
        spec = importlib.util.spec_from_file_location(
            "isl_" + transport + "_test", ROOT / "lambdas" / script
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        resource = mocker.Mock()
        sqs = mocker.Mock()
        sqs.delete_message_batch.return_value = {"Successful": [{"Id": "message-1"}]}
        mocker.patch.object(module.boto3, "resource", return_value=resource)
        mocker.patch.object(module.boto3, "client", return_value=sqs)
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"success": true, "result": "test-job"}'
        post = mocker.patch.object(module.requests, "post", return_value=response)
        return module, resource, sqs, post

    return load


def event_for(transport, key):
    message = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "test-bucket"},
                    "object": {"key": quote_plus(key, safe="/"), "eTag": "test-etag"},
                }
            }
        ]
    }
    if transport == "sns":
        return {"Records": [{"Sns": {"Message": json.dumps(message)}}]}
    return {
        "Records": [
            {
                "body": json.dumps(message),
                "messageId": "message-1",
                "receiptHandle": "receipt-1",
                "eventSourceARN": "arn:aws:sqs:us-west-2:123456789012:isl",
            }
        ]
    }


def job_parameters(post):
    assert post.call_count == 1
    return json.loads(post.call_args.kwargs["data"]["params"])


@pytest.mark.parametrize("transport", ["sqs", "sns"])
@pytest.mark.parametrize(
    "key",
    [
        "data/plain.nc",
        "data/red flower.nc",
        "data/a+b.nc",
        "data/100%25.nc",
        "data/café.nc",
        "data/literal%2B.nc",
    ],
)
def test_ingest_jobs_use_decoded_object_keys(load_handler, transport, key):
    module, resource, sqs, post = load_handler(transport)
    event = event_for(transport, key)
    original = deepcopy(event)
    module.lambda_handler(event, None)
    params = job_parameters(post)
    url = "s3://s3-us-west-2.amazonaws.com/test-bucket/" + key
    assert params["data_url"] == ([url] if transport == "sqs" else url)
    assert params["data_file"] == key.rsplit("/", 1)[-1]
    assert params["id"] == params["data_file"]
    assert params["prod_met"]["ISL_urls"] == [url]
    assert params["prod_met"]["S3_event_record"]["s3"]["object"]["key"] == quote_plus(
        key, safe="/"
    )
    assert event == original
    if transport == "sqs":
        resource.Object.assert_called_once_with("test-bucket", key)
        assert params["payload_hash"] == "test-etag"
        sqs.delete_message_batch.assert_called_once_with(
            QueueUrl="https://sqs.us-west-2.amazonaws.com/123456789012/isl",
            Entries=[{"Id": "message-1", "ReceiptHandle": "receipt-1"}],
        )


@pytest.mark.parametrize("transport", ["sqs", "sns"])
def test_decoded_filename_selects_the_configured_job(load_handler, transport):
    module, _, _, post = load_handler(transport)
    os.environ["JOB_TYPES"] = json.dumps(
        {
            "special-ingest": {
                "PATTERN": r"^a\+b[.]nc$",
                "RELEASE": "special",
                "QUEUE": "special-queue",
            }
        }
    )
    module.lambda_handler(event_for(transport, "data/a+b.nc"), None)
    assert post.call_args.kwargs["data"]["type"] == "job-special-ingest:special"
    assert post.call_args.kwargs["data"]["queue"] == "special-queue"


def test_signal_file_is_read_using_the_decoded_s3_key(load_handler):
    module, resource, _, post = load_handler(
        "sqs", {"met_required": {"ext": ".signal"}}
    )
    key = "met_required/granule + one.signal"
    resource.Object.return_value.get.return_value = {
        "Body": BytesIO(b"data + one.nc\n\n")
    }
    module.lambda_handler(event_for("sqs", key), None)
    assert all(
        call.args == ("test-bucket", key) for call in resource.Object.call_args_list
    )
    assert resource.Object.call_count == 2
    params = job_parameters(post)
    base = "s3://s3-us-west-2.amazonaws.com/test-bucket/met_required/"
    assert params["data_url"] == [base + "data + one.nc"]
    assert params["prod_met"]["ISL_urls"] == [
        base + "data + one.nc",
        base + "granule + one.signal",
    ]
    assert params["prod_met"]["restaged"] is True


def test_sns_signal_suffix_is_removed_after_decoding(load_handler):
    module, _, _, post = load_handler("sns", ".signal")
    module.lambda_handler(event_for("sns", "data/granule + one.nc.signal"), None)
    assert job_parameters(post)["data_file"] == "granule + one.nc"
    assert job_parameters(post)["data_url"].endswith("/data/granule + one.nc")


def test_failed_job_submission_still_prevents_sqs_deletion(load_handler):
    module, _, sqs, post = load_handler("sqs")
    post.side_effect = requests.exceptions.ConnectionError("synthetic failure")
    with pytest.raises(requests.exceptions.ConnectionError, match="synthetic failure"):
        module.lambda_handler(event_for("sqs", "data/red flower.nc"), None)
    sqs.delete_message_batch.assert_not_called()
