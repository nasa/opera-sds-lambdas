import datetime
import importlib
import os
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from _pytest.monkeypatch import MonkeyPatch

event = {
        "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
        "detail-type": "Scheduled Event",
        "source": "aws.events",
        "account": "123456789012",
        "time": "1970-01-01T00:00:00Z",
        "region": "us-east-1",
        "resources": [
            "arn:aws:events:us-east-1:123456789012:rule/ExampleRule"
        ],
        "detail": {}
    }

os.environ = {
    "MOZART_URL": "dummy_mozart_url",
    "MINUTES": "60",
    "REVISION_START_DATETIME_MARGIN_MINS": "0",
    "PROVIDER": "dummy_provider",
    "JOB_TYPE": "dummy_job_type",
    "JOB_RELEASE": "dummy_job_release",
    "JOB_QUEUE": "dummy_job_queue",
    "ISL_BUCKET_NAME": "dummy_isl_bucket",
    "ENDPOINT": "dummy_endpoint",
    "DOWNLOAD_JOB_QUEUE": "dummy_download_job_queue",
    "CHUNK_SIZE": "dummy_chunk_size",
    "MAX_REVISION": "1000",
    "SMOKE_RUN": "true",
    "DRY_RUN": "true",
    "NO_SCHEDULE_DOWNLOAD": "true"
}

data_subscriber_query = importlib.import_module("lambdas.data-subscriber-query.data_subscriber_query_lambda")



def test_lambda_handler(mocker: MockerFixture, monkeypatch: MonkeyPatch):
    # ARRANGE
    context = MagicMock()
    mocker.patch(data_subscriber_query.__name__ + ".submit_job", return_value=200)
    monkeypatch.setenv("USE_TEMPORAL", "false")

    # ACT
    response = data_subscriber_query.lambda_handler(event, context)

    # ASSERT
    assert response == 200


def test_get_temporal_start_datetime__when_USE_TEMPORAL_is_empty_string__and_no_temporal_value_given__then_returns_empty_string(monkeypatch):
    # ARRANGE
    monkeypatch.setenv("USE_TEMPORAL", "")

    # ACT
    temporal_start_datetime = data_subscriber_query.get_temporal_start_datetime(datetime.datetime.strptime("2023-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"))

    # ASSERT
    assert temporal_start_datetime == ""


def test_get_temporal_start_datetime__when_USE_TEMPORAL_is_false__and_no_temporal_value_given__then_returns_empty_string(monkeypatch):
    # ARRANGE
    monkeypatch.setenv("USE_TEMPORAL", "false")

    # ACT
    temporal_start_datetime = data_subscriber_query.get_temporal_start_datetime(datetime.datetime.strptime("2023-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"))

    # ASSERT
    assert temporal_start_datetime == ""


def test_get_temporal_start_datetime__when_USE_TEMPORAL_is_true__but_no_temporal_value_given__then_returns_empty_string(monkeypatch):
    # ARRANGE
    monkeypatch.setenv("USE_TEMPORAL", "true")

    # ACT
    temporal_start_datetime = data_subscriber_query.get_temporal_start_datetime(datetime.datetime.strptime("2023-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"))

    # ASSERT
    assert temporal_start_datetime == ""


def test_get_temporal_start_datetime__when_margin_given__then_returns_updated_datetime(monkeypatch):
    # ARRANGE
    monkeypatch.setenv("USE_TEMPORAL", "true")
    monkeypatch.setenv("TEMPORAL_START_DATETIME_MARGIN_DAYS", 3)

    # ACT
    temporal_start_datetime = data_subscriber_query.get_temporal_start_datetime(datetime.datetime.strptime("2023-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"))

    # ASSERT
    assert temporal_start_datetime == "2022-12-29T00:00:00Z"


def test_get_temporal_start_datetime__when_datetime_given__then_returns_updated_datetime(monkeypatch):
    # ARRANGE
    monkeypatch.setenv("USE_TEMPORAL", "true")
    monkeypatch.setenv("TEMPORAL_START_DATETIME", "2022-12-29T00:00:00Z")

    # ACT
    temporal_start_datetime = data_subscriber_query.get_temporal_start_datetime(datetime.datetime.strptime("2023-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"))

    # ASSERT
    assert temporal_start_datetime == "2022-12-29T00:00:00Z"


def test_get_temporal_start_datetime__when_both_margin_and_datetime_are_given__then_uses_margin(monkeypatch):
    # ARRANGE
    monkeypatch.setenv("USE_TEMPORAL", "true")
    monkeypatch.setenv("TEMPORAL_START_DATETIME_MARGIN_DAYS", 3)
    monkeypatch.setenv("TEMPORAL_START_DATETIME", "1970-01-01T00:00:00Z")

    # ACT
    temporal_start_datetime = data_subscriber_query.get_temporal_start_datetime(datetime.datetime.strptime("2023-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"))

    # ASSERT
    assert temporal_start_datetime == "2022-12-29T00:00:00Z"

def test_create_job_simple(monkeypatch):
    # ARRANGE
    monkeypatch.setenv("USE_TEMPORAL", "false")

    job_name, job_spec, job_params, queue, tags = data_subscriber_query._create_job(event)

    job_type = os.environ["JOB_TYPE"]
    job_release = os.environ["JOB_RELEASE"]
    assert job_spec == f"job-{job_type}:{job_release}"

    assert job_params['start_datetime'] == '--start-date='+'1969-12-31T23:00:00Z'
    assert job_params['end_datetime'] == '--end-date=' + '1970-01-01T00:00:00Z'

def test_create_job_revision_margin(monkeypatch):
    # ARRANGE
    monkeypatch.setenv("USE_TEMPORAL", "false")
    monkeypatch.setenv("REVISION_START_DATETIME_MARGIN_MINS", "60")
    monkeypatch.setenv("MAX_REVISION", "3")

    job_name, job_spec, job_params, queue, tags = data_subscriber_query._create_job(event)

    job_type = os.environ["JOB_TYPE"]
    job_release = os.environ["JOB_RELEASE"]
    assert job_spec == f"job-{job_type}:{job_release}"

    assert job_params['start_datetime'] == '--start-date='+'1969-12-31T22:00:00Z'
    assert job_params['end_datetime'] == '--end-date=' + '1969-12-31T23:00:00Z'
    assert job_params['max_revision'] == "--max-revision=3"