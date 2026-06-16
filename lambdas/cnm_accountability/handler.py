import json
import logging
import os
from datetime import datetime, timedelta, timezone
from io import StringIO

import boto3
import opensearchpy
from opensearchpy.helpers import scan
from tabulate import tabulate

import queries as q

if len(logging.getLogger().handlers) > 0:
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
    )

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


PRODUCT_INDEX_MAP = {
    'L3_DSWX_HLS': 'grq_*_l3_dswx_hls-*',
    'L2_RTC_S1': 'grq_*_l2_rtc_s1-*',
    'L2_CSLC_S1': 'grq_*_l2_cslc_s1-*',
    'L3_DSWX_S1': 'grq_*_l3_dswx_s1-*',
    'L3_DISP_S1': 'grq_*_l3_disp_s1-*',
    'L4_TROPO': 'grq_*_l4_tropo-*',
    # 'L3_DSWX_NI': 'grq_*_l3_dswx_ni-*',
    'L3_DIST_S1': 'grq_*_l3_dist_s1-*',
    # 'L3_DISP_NI': 'grq_*_l3_disp_ni-*',
    # 'L4_CAL_DISP': 'grq_*_l4_cal_disp-*',
    'L2_RTC_S1_STATIC': 'grq_*_l2_rtc_s1_static-*',
    'L2_CSLC_S1_STATIC': 'grq_*_l2_cslc_s1_static-*',
    'L3_DISP_S1_STATIC': 'grq_*_l3_disp_s1_static-*',
}

TOTAL_PRODUCTS = 'TOTAL_PRODUCTS'
CNM_UNSENT = 'CNM_UNSENT'
CNM_SEND_FAILED = 'CNM_SEND_FAILED'
CNM_NO_RESPONSE = 'CNM_NO_RESPONSE'
CNM_INGEST_FAILED = 'CNM_INGEST_FAILED'

COL_NAME_MAP = {
    TOTAL_PRODUCTS: 'Total Products',
    CNM_UNSENT: 'CNM-S Unsent',
    CNM_SEND_FAILED: 'CNM-S Failed',
    CNM_NO_RESPONSE: 'No DAAC Response',
    CNM_INGEST_FAILED: 'Ingest Failed',
}

TIME_FMT = '%Y-%m-%dT%H:%M:%SZ'

DELETE_SCROLLS = True
"""Delete ElasticSearch scroll contexts when finished, or just let them expire"""


def get_time_range(start_days_back=0, range_size=1):
    start_date = (datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) -
                  timedelta(days=start_days_back))
    return start_date - timedelta(days=range_size), start_date


def get_grq_client(host) -> opensearchpy.OpenSearch:
    client = opensearchpy.OpenSearch(
        [host] if isinstance(host, str) else host,
        timeout=30,
        max_retries=10,
        retry_on_timeoout=True,
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
    )

    return client


def query_for_ids(client, index_pattern, query, transform=None):
    def id_from_doc(doc):
        return doc['id']

    if transform is None:
        transform = id_from_doc

    return [transform(doc['_source']) for doc in scan(client, query, index=index_pattern,
                                                      size=10_000, clear_scroll=DELETE_SCROLLS)]


def get_cnm_accountability_for_product(client: opensearchpy.OpenSearch, index_pattern: str,
                                       start: datetime, end: datetime):
    return {
        TOTAL_PRODUCTS: client.count(
            index=index_pattern, body=q.add_time_range(q.BLANK_QUERY, start, end)
        )['count'],
        CNM_UNSENT: query_for_ids(
            client, index_pattern, q.add_time_range(q.CNM_NOT_SENT, start, end)
        ),
        CNM_SEND_FAILED: query_for_ids(
            client, index_pattern, q.add_time_range(q.CNM_SEND_FAILED, start, end)
        ),
        CNM_NO_RESPONSE: query_for_ids(
            client, index_pattern, q.add_time_range(q.CNM_NO_RESPONSE, start, end)
        ),
        CNM_INGEST_FAILED: query_for_ids(
            client, index_pattern, q.add_time_range(q.CNM_INGEST_FAILED, start, end), transform=lambda x: {
                'id': x['id'],
                'error_code': x['daac_delivery_status'],
                'error_message': x['daac_delivery_error_message']
            }
        ),
    }


def report(accountability, start: datetime, end: datetime, venue, debug=False):
    # Map product type -> {count type -> count}
    counts = {
        product_type: {
            count_type: len(accountability[product_type][count_type]) if
            isinstance(accountability[product_type][count_type], list) else
            accountability[product_type][count_type]
            for count_type in accountability[product_type]
        } for product_type in accountability
    }

    # Remove products not generated at all from the report
    for product_type in counts:
        if accountability[product_type][TOTAL_PRODUCTS] == 0:
            del accountability[product_type]

    # Remove empty counts from the report
    for product_type in accountability:
        for count_type in list(accountability[product_type].keys()):
            if isinstance(accountability[product_type][count_type], list):
                if len(accountability[product_type][count_type]) == 0:
                    del accountability[product_type][count_type]

    def _insert_products_header(table_str, header_row=0, sep_chars=' |', insert_str='Product'):
        lines = table_str.split('\n')
        row = lines[header_row]
        idx = row.index(sep_chars)
        row_after = row[idx:]
        row_before = row[:idx]
        keep_chars = len(row_before) - len(insert_str)
        new_row = row_before[:keep_chars] + insert_str + row_after
        lines[header_row] = new_row
        return '\n'.join(lines)

    txt_report = StringIO()

    report_title = (f'OPERA DAAC Delivery Accountability Report [{venue}] | '
                    f'{start.strftime(TIME_FMT)} to {end.strftime(TIME_FMT)}')

    txt_report.write(
        f'{report_title}\n\n'
    )

    txt_report.write(_insert_products_header(tabulate(
        counts.values(),
        showindex=counts.keys(),
        headers=COL_NAME_MAP,
        tablefmt='grid',
        intfmt=','
    ), header_row=1))

    header = True
    for product_type in accountability:
        if CNM_INGEST_FAILED in accountability[product_type]:
            if header:
                txt_report.write('\n\n-------------Ingestion Failures-------------')
                header = False
            txt_report.write(f'\n\nProduct type: {product_type} ({len(accountability[product_type][CNM_INGEST_FAILED]):,})')
            # TODO: Sort by severity?
            for i, failure in enumerate(accountability[product_type][CNM_INGEST_FAILED]):
                if i >= 5:
                    txt_report.write(f'\n  (skipping {len(accountability[product_type][CNM_INGEST_FAILED]) - i} '
                                     f'additional products for brevity - see attached JSON report for full list)')
                    break

                txt_report.write(f'\n  - ID: {failure["id"]}')
                txt_report.write(f'\n    - Error Code:    {failure["error_code"]}')
                txt_report.write(f'\n    - Error Message: {failure["error_message"]}')

    if header:
        txt_report.write('\n\nNo ingestion failures found!')

    print(txt_report.getvalue())

    json_report_data = json.dumps(accountability, indent=2).encode('utf-8')

    if debug:
        with open('test.json', 'w') as f:
            json.dump(accountability, f, indent=2)

    # Transform the counts dict into a structure more suitable to rendering the report HTML template
    html_report_rows = []
    html_ingestion_failures = []

    for product_type in counts:
        row_dict = {'product': product_type}

        product_count = counts[product_type][TOTAL_PRODUCTS]

        for count in counts[product_type]:
            count_value = counts[product_type][count]
            row_dict: dict[str, dict[str, str | int]]
            row_dict[count] = {'value': f'{count_value:,}'}

            if count_value > 0:
                percent_count = float(count_value) / float(product_count)

                if count in {CNM_INGEST_FAILED, CNM_SEND_FAILED}:
                    row_dict[count]['value'] += f' ({percent_count * 100:.3f}%)'
                    row_dict[count]['severity'] = 3
                elif count != TOTAL_PRODUCTS:
                    row_dict[count]['value'] += f' ({percent_count * 100:.3f}%)'
                    if percent_count >= 0.15:
                        row_dict[count]['severity'] = 3
                    elif percent_count >= 0.10:
                        row_dict[count]['severity'] = 2
                    elif percent_count >= 0.05:
                        row_dict[count]['severity'] = 1
                    else:
                        row_dict[count]['severity'] = 0
            else:
                row_dict[count]['severity'] = 0

        html_report_rows.append(row_dict)

    for product_type in accountability:
        if CNM_INGEST_FAILED in accountability[product_type]:
            product_failure_dict = {
                'product': product_type,
                'product_count': f'{len(accountability[product_type][CNM_INGEST_FAILED])}',
                'products': [{
                    'id': failure['id'],
                    'error_code': failure['error_code'],
                    'error_message': failure['error_message']}
                    for failure in accountability[product_type][CNM_INGEST_FAILED][:5]],
            }

            if len(product_failure_dict['products']) != len(accountability[product_type][CNM_INGEST_FAILED]):
                product_failure_dict['skipped'] = len(accountability[product_type][CNM_INGEST_FAILED]) - len(product_failure_dict['products'])
                product_failure_dict['skipped_str'] = f"{product_failure_dict['skipped']:,}"
            else:
                product_failure_dict['skipped'] = 0
            html_ingestion_failures.append(product_failure_dict)

    html_report_str = None

    try:
        import jinja2

        template_directory = os.getcwd()
        template_filename = 'report_template.html.jinja2'

        template_loader = jinja2.FileSystemLoader(template_directory)

        template_env = jinja2.Environment(loader=template_loader,
                                          autoescape=jinja2.select_autoescape())

        template = template_env.get_template(template_filename)
        html_report_str = template.render({
            'rows': html_report_rows,
            'colors': {
                1: 'yellow',
                2: '#FFC000',
                3: 'red',
            },
            'venue': venue,
            'start': start.strftime(TIME_FMT),
            'end': end.strftime(TIME_FMT),
            'ingestion_failures': html_ingestion_failures,
        })

        if debug:
            with open('test_render.html', 'w') as f:
                f.write(html_report_str)
    except Exception as e:
        print(e)

    return txt_report.getvalue(), html_report_str, json_report_data, report_title


def lambda_handler(event, context):
    grq_url = os.environ['GRQ_URL']
    venue = os.environ['VENUE']

    grq = get_grq_client(grq_url)
    assert grq.ping(), f'Cannot reach GRQ cluster at {grq_url}'

    start_days_back = int(os.getenv('WINDOW_START_DAYS_BACK', -1))
    range_size = int(os.getenv('WINDOW_SIZE_IN_DAYS', 1))

    assert start_days_back >= 0
    assert range_size >= 1

    start, end = get_time_range(start_days_back, range_size)

    cnn_accountability = {product_type: get_cnm_accountability_for_product(grq, pattern, start, end)
                          for product_type, pattern in PRODUCT_INDEX_MAP.items()}

    plaintext_report, html_report, json_bytes, report_title = report(cnn_accountability, start, end, venue)

    ses = boto3.client('sesv2')

    sender = os.environ['REPORT_SENDER_EMAIL']

    recipients = os.environ['REPORT_RECIPIENT_EMAILS']
    cc = os.getenv('REPORT_CC_EMAILS', None)
    bcc = os.getenv('REPORT_BCC_EMAILS', None)

    dst = {
        'ToAddresses': [ea.strip() for ea in recipients.split(',')]
    }

    if cc:
        dst['CcAddresses'] = [ea.strip() for ea in cc.split(',')]
    if bcc:
        dst['BccAddresses'] = [ea.strip() for ea in bcc.split(',')]

    try:
        resp = ses.send_email(
            FromEmailAddress=sender,
            Destination=dst,
            ReplyToAddresses=[sender],
            Content={
                'Simple': {
                    'Subject': {
                        'Data': report_title,
                    },
                    'Body': {
                        'Text': {
                            'Data': plaintext_report
                        },
                        'Html': {
                            'Data': html_report,
                        }
                    },
                    'Attachments': [
                        {
                            'RawContent': json_bytes,
                            'ContentDisposition': 'ATTACHMENT',
                            'FileName': 'report.json',
                            'ContentType': 'application/json',
                        }
                    ]
                }
            }
        )

        return resp
    except Exception as e:
        logger.critical(e)
        raise e


def main_dev():
    from getpass import getpass, getuser
    from urllib.parse import urlparse, urlunparse

    username = getuser()
    password = getpass('JPL Password: ')
    grq_url, venue = '<https://<MOZART>/grq_es>', 'VenueName'

    p_url = list(urlparse(grq_url))
    p_url[1] = f'{username}:{password}@' + p_url[1]
    orig_grq_url = grq_url
    grq_url = urlunparse(p_url)

    grq = get_grq_client(grq_url)
    assert grq.ping(), f'Cannot reach GRQ cluster at {orig_grq_url}'

    start, end = get_time_range(range_size=90, start_days_back=-1)

    # cnn_accountability = {product_type: get_cnm_accountability_for_product(grq, pattern, start, end)
    #                       for product_type, pattern in PRODUCT_INDEX_MAP.items()}

    with open('acc_with_failures.json') as fp:
        cnn_accountability = json.load(fp)

    report(cnn_accountability, start, end, venue, debug=True)


if __name__ == '__main__':
    main_dev()
