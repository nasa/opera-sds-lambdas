import importlib
import os
from unittest.mock import MagicMock

from pytest_mock import MockerFixture

os.environ = {
    "MOZART_URL": "dummy_mozart_url",
    "MINUTES": "60",
    "PROVIDER": "dummy_provider",
    "JOB_TYPE": "dummy_job_type",
    "JOB_RELEASE": "dummy_job_release",
    "JOB_QUEUE": "dummy_job_queue",
    "ISL_BUCKET_NAME": "dummy_isl_bucket",
    "ENDPOINT": "dummy_endpoint",
    "DOWNLOAD_JOB_QUEUE": "dummy_download_job_queue",
    "CHUNK_SIZE": "dummy_chunk_size",
    "SMOKE_RUN": "true",
    "DRY_RUN": "true",
    "NO_SCHEDULE_DOWNLOAD": "true"
}

data_subscriber_query = importlib.import_module("lambdas.data-subscriber-query.data_subscriber_query_lambda")


def test_lambda_handler(mocker: MockerFixture, ):
    # ARRANGE
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
    context = MagicMock()
    mocker.patch(data_subscriber_query.__name__ + ".submit_job", return_value=200)

    # ACT
    response = data_subscriber_query.lambda_handler(event, context)

    # ASSERT
    assert response == 200
