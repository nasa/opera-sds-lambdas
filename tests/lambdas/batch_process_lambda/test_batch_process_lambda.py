from datetime import datetime
import importlib
import os
import pytest

batch_lambda = importlib.import_module("lambdas.batch_process.batch_process_lambda")

START_DATE = '2020-12-31T23:00:00Z'
END_DATE = '2021-12-31T23:00:00Z'
PROCESSING_MODE = "historical"
JOB_TYPE = "hlss30_query"
INCLUDE_REGIONS = "north_america_opera"
EXCLUDE_REGIONS = "california"

class P(object):
    pass

def generate_p():
    p = P()
    p.label = "historical_1"
    p.processing_mode = PROCESSING_MODE
    p.job_type = JOB_TYPE
    p.download_job_queue = "some_queue"
    p.chunk_size = 1
    p.include_regions = INCLUDE_REGIONS
    p.exclude_regions = EXCLUDE_REGIONS

    return p
def generate_p_hls():
    p = generate_p()

    p.job_queue = "opera-job_worker-hls_data_query"
    p.collection_short_name = "HLSS30"

    return p

def generate_p_disp():
    p = generate_p()

    p.job_queue = "opera-job_worker-disp_data_query"
    p.collection_short_name = "OPERA_L2_CSLC-S1_V1"
    p.k = 2
    p.frames_per_query = 100

    return p

def test_lambda_handler_hls():

    p = generate_p_hls()
    s_date = datetime.strptime(START_DATE, batch_lambda.DATETIME_FORMAT)
    e_date = datetime.strptime(END_DATE, batch_lambda.DATETIME_FORMAT)
    (job_name, job_spec, job_params, job_tags) = batch_lambda.form_job_params(p, s_date, e_date, None)

    # ASSERT
    assert job_name == "data-subscriber-query-timer-historical_1_2020-12-31T23:00:00-2021-12-31T23:00:00"
    assert JOB_TYPE in job_spec
    assert job_tags == ['data-subscriber-query-timer', 'historical_processing']
    assert job_params["start_datetime"] == f"--start-date={START_DATE}"
    assert job_params["end_datetime"] == f"--end-date={END_DATE}"
    assert job_params["processing_mode"] == f'--processing-mode={PROCESSING_MODE}'
    assert job_params["use_temporal"] == f'--use-temporal'
    assert job_params["include_regions"] == f'--include-regions={INCLUDE_REGIONS}'
    assert job_params["exclude_regions"] == f'--exclude-regions={EXCLUDE_REGIONS}'

def test_lambda_handler_disp():

    map, metadata, version = batch_lambda.process_disp_frame_burst_json(batch_lambda.DISP_FRAME_BURST_MAP_JSON)

    p = generate_p_disp()
    s_date = datetime.strptime(START_DATE, batch_lambda.DATETIME_FORMAT)
    e_date = batch_lambda.get_e_date(s_date, p)
    (job_name, job_spec, job_params, job_tags) = batch_lambda.form_job_params(p, s_date, e_date, map)

    # ASSERT
    assert job_name == "data-subscriber-query-timer-historical_1_2020-12-31T23:00:00-2021-01-24T23:00:00"
    assert JOB_TYPE in job_spec
    assert job_tags == ['data-subscriber-query-timer', 'historical_processing']
    assert job_params["start_datetime"] == f"--start-date={START_DATE}"
    assert job_params["end_datetime"] == f"--end-date=2021-01-24T23:00:00Z"
    assert job_params["processing_mode"] == f'--processing-mode={PROCESSING_MODE}'
    assert job_params["use_temporal"] == f'--use-temporal'
    assert job_params["include_regions"] == f'--include-regions={INCLUDE_REGIONS}'
    assert job_params["exclude_regions"] == f'--exclude-regions={EXCLUDE_REGIONS}'
    assert job_params["frame_range"] == f'--frame-range=1,100'
