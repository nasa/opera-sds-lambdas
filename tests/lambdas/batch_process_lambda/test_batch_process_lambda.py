from datetime import datetime
import importlib
import os
import pytest

batch_lambda = importlib.import_module("lambdas.batch_process.batch_process_lambda")

START_DATE = '2020-12-31T23:00:00Z'
END_DATE = '2021-12-31T23:00:00Z'
PROCESSING_MODE = "historical"
JOB_TYPE = "slcs1a_query"
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
    p.data_start_date = START_DATE[:-1]
    p.data_end_date = END_DATE[:-1]
    p.last_successful_proc_data_date = '2000-01-01T00:00:00'

    return p
def generate_p_slc():
    p = generate_p()

    p.job_queue = "opera-job_worker-slc_data_query"
    p.collection_short_name = "SENTINEL-1A_SLC"
    p.data_date_incr_mins = 120

    return p

def generate_p_disp():
    p = generate_p()

    p.job_queue = "opera-job_worker-disp_data_query"
    p.collection_short_name = "OPERA_L2_CSLC-S1_V1"
    p.k = 2
    p.frames_per_query = 100

    return p

def test_lambda_handler_slc():

    p = generate_p_slc()
    job_name, job_spec, job_params, job_tags, last_proc_date, last_proc_frame, finished = \
        batch_lambda.form_job_params(p, None)

    assert job_name == "data-subscriber-query-timer-historical_1_2020-12-31T23:00:00-2021-01-01T01:00:00"
    assert JOB_TYPE in job_spec
    assert job_tags == ['data-subscriber-query-timer', 'historical_processing']
    assert job_params["start_datetime"] == f"--start-date={START_DATE}"
    assert job_params["end_datetime"] == f"--end-date=2021-01-01T01:00:00Z"
    assert job_params["processing_mode"] == f'--processing-mode={PROCESSING_MODE}'
    assert job_params["use_temporal"] == f'--use-temporal'
    assert job_params["include_regions"] == f'--include-regions={INCLUDE_REGIONS}'
    assert job_params["exclude_regions"] == f'--exclude-regions={EXCLUDE_REGIONS}'
    assert finished == False

def test_lambda_handler_slc_near_last():

    p = generate_p_slc()
    p.data_start_date = '2021-12-31T23:00:00'
    p.data_end_date =   '2021-12-31T23:30:00'
    job_name, job_spec, job_params, job_tags, last_proc_date, last_proc_frame, finished = \
        batch_lambda.form_job_params(p, None)

    assert job_tags == ['data-subscriber-query-timer', 'historical_processing']
    assert job_params["start_datetime"] == f"--start-date=2021-12-31T23:00:00Z"
    assert job_params["end_datetime"] == f"--end-date=2021-12-31T23:30:00Z"
    assert finished == False

def test_lambda_handler_slc_near_last():

    p = generate_p_slc()
    p.data_start_date = '2021-12-31T23:00:00'
    p.data_end_date =   '2021-12-31T23:00:00'
    job_name, job_spec, job_params, job_tags, last_proc_date, last_proc_frame, finished = \
        batch_lambda.form_job_params(p, None)

    assert finished == True
def test_lambda_handler_disp():

    map, metadata, version = batch_lambda.process_disp_frame_burst_json(batch_lambda.DISP_FRAME_BURST_MAP_JSON)

    p = generate_p_disp()
    job_name, job_spec, job_params, job_tags, last_proc_date, last_proc_frame, finished = \
        batch_lambda.form_job_params(p, map)

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
    assert last_proc_frame == 100
    assert last_proc_date == batch_lambda.convert_datetime(START_DATE)
    assert finished == False

def test_lambda_handler_disp_before_rollover():

    map, metadata, version = batch_lambda.process_disp_frame_burst_json(batch_lambda.DISP_FRAME_BURST_MAP_JSON)

    max_frame = len(map)

    p = generate_p_disp()
    p.last_successful_proc_frame = max_frame - 2
    job_name, job_spec, job_params, job_tags, last_proc_date, last_proc_frame, finished = \
        batch_lambda.form_job_params(p, map)

    assert job_params["start_datetime"] == f"--start-date={START_DATE}"
    assert job_params["end_datetime"] == f"--end-date=2021-01-24T23:00:00Z"
    assert job_params["frame_range"] == f'--frame-range={p.last_successful_proc_frame+1},{max_frame}'
    finished == False

def test_lambda_handler_disp_rollover():

    map, metadata, version = batch_lambda.process_disp_frame_burst_json(batch_lambda.DISP_FRAME_BURST_MAP_JSON)

    max_frame = len(map)

    p = generate_p_disp()
    p.last_successful_proc_frame = max_frame
    job_name, job_spec, job_params, job_tags, last_proc_date, last_proc_frame, finished = \
        batch_lambda.form_job_params(p, map)

    assert job_params["start_datetime"] == f"--start-date=2021-01-24T23:00:00Z"
    assert job_params["end_datetime"] == f"--end-date=2021-02-17T23:00:00Z"
    assert job_params["frame_range"] == f'--frame-range=1,100'
    finished == False

def test_lambda_handler_disp_near_last():

    map, metadata, version = batch_lambda.process_disp_frame_burst_json(batch_lambda.DISP_FRAME_BURST_MAP_JSON)

    max_frame = len(map)

    p = generate_p_disp()
    p.last_successful_proc_frame = max_frame - 1
    p.data_start_date = '2021-12-06T23:00:00'
    p.data_end_date = '2021-12-31T23:00:00'
    job_name, job_spec, job_params, job_tags, last_proc_date, last_proc_frame, finished = \
        batch_lambda.form_job_params(p, map)

    assert job_params["start_datetime"] == f"--start-date=2021-12-06T23:00:00Z"
    assert job_params["end_datetime"] == f"--end-date=2021-12-30T23:00:00Z"
    assert job_params["frame_range"] == f'--frame-range={max_frame},{max_frame}'
    assert finished == False
def test_lambda_handler_disp_last():

    map, metadata, version = batch_lambda.process_disp_frame_burst_json(batch_lambda.DISP_FRAME_BURST_MAP_JSON)

    max_frame = len(map)

    p = generate_p_disp()
    p.last_successful_proc_frame = max_frame
    p.data_start_date = '2021-12-30T23:00:00'
    p.data_end_date = '2021-12-31T23:00:00'
    job_name, job_spec, job_params, job_tags, last_proc_date, last_proc_frame, finished = \
        batch_lambda.form_job_params(p, map)

    assert finished == True