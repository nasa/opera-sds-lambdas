import json
import logging
import os
import shutil
import zipfile
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse

import boto3
import jinja2
from zoneinfo import ZoneInfo


if len(logging.getLogger().handlers) > 0:
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
    )

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


TWO_SIGMA_THRESHOLD = 2
PRODUCT_COUNT_COLS = 3

TOTAL_PRODUCTS = 'TOTAL_PRODUCTS'
CNM_UNSENT = 'CNM_UNSENT'
CNM_SEND_FAILED = 'CNM_SEND_FAILED'
CNM_NO_RESPONSE = 'CNM_NO_RESPONSE'
CNM_INGEST_FAILED = 'CNM_INGEST_FAILED'

ATTACHMENT_SIZE_THRESHOLD = 30 * (1024 ** 2)


PRODUCT_ORDER = {p: i for i, p in enumerate([
    'DSWx-HLS',
    'CSLC-S1',
    'RTC-S1',
    'DSWx-S1',
    'DISP-S1',
    'DIST-S1',
    'TROPO',
    'DSWx-NI',
    'DISP-NI'
])}


s3 = boto3.client('s3')


def _get_items_for_prefix(d: dict, prefix: str, exclude: tuple = None, sort=None):
    if sort:
        keys = sorted(d.keys(), key=sort)
    else:
        keys = d.keys()

    for key in keys:
        if key.startswith(prefix):
            if exclude and any(key.endswith(e) for e in exclude):
                continue
            yield key, d[key]


def _get_attachment_by_filename(attachments, filename):
    a = None

    for attachment in attachments:
        if attachment['FileName'] == filename:
            a = attachment
            break

    return a


def _get_attachments_by_content_type(attachments, content_type):
    ret = []

    for attachment in attachments:
        if attachment['ContentType'] == content_type:
            ret.append(attachment)

    return ret


def render_html_report(data, debug=False, local_path=False):
    jinja_payload = {
        'general': {
            'tag': data['tag'],
            'venue': data['venue'].upper(),
            'gather_job_start_time': data['data_gather_start_time'],
            'gather_job_end_time': data['data_gather_end_time'],
            'gather_job_id': data['data_gather_job_id'],
        },
        'product_counts': {},
        'latencies': {},
        'product_accountability': [],
        'duplicates': [],
        'daac_accountability': {},
        'local_paths': bool(local_path),
        'timestamp_job': datetime.now(ZoneInfo('America/Los_Angeles')).strftime("%Y-%m-%d %H:%H:%S %Z"),
        'timestamp_report': datetime.strptime(
            data['data_gather_end_time'],
            '%Y-%m-%dT%H:%M:%SZ'
        ).replace(tzinfo=ZoneInfo('UTC')).astimezone(ZoneInfo('America/Los_Angeles')).strftime("%Y-%m-%d %H:%H:%S %Z"),
    }

    data = data['report_data']

    # Product counts

    source_metadata = data['SDS_Status']

    jinja_payload['product_counts']['error_count'] = len(source_metadata['errors'])
    jinja_payload['product_counts']['errors'] = source_metadata['errors']
    jinja_payload['latencies']['error_count'] = len(source_metadata['errors'])
    jinja_payload['latencies']['errors'] = source_metadata['errors']

    if not source_metadata['errors']:
        try:
            jinja_payload['product_counts']['start'] = source_metadata['data']['data_from']
            jinja_payload['product_counts']['end'] = source_metadata['data']['data_to']
            jinja_payload['product_counts']['report_time'] = source_metadata['data']['report_time']
            jinja_payload['product_counts']['products'] = []

            for product in source_metadata['data']['products']:
                product_dict = {
                    'product': product,
                    'mean': f"{source_metadata['data']['products'][product]['mean']:,.2f}",
                    'minus_2_sigma': f"{source_metadata['data']['products'][product]['-2sigma']:,.2f}",
                    'plus_2_sigma': f"{source_metadata['data']['products'][product]['+2sigma']:,.2f}",
                    'alert_no_data': source_metadata['data']['products'][product]['no_data'],
                    'alert_below_2_sigma': source_metadata['data']['products'][product]['recent_below_2_sigma'] >= TWO_SIGMA_THRESHOLD,
                    'below_2_sigma_count': source_metadata['data']['products'][product]['recent_below_2_sigma'],
                }

                plot_attachment = _get_attachment_by_filename(source_metadata['attachments'], f'opera_daily_products_query_{product}.png')

                if plot_attachment is None:
                    raise Exception(f'No plot attachment found for product {product}')

                if bool(local_path):
                    if isinstance(local_path, str) and os.path.isdir(local_path):
                        product_dict['plot_src'] = os.path.join(local_path, plot_attachment['FileName'])
                    else:
                        product_dict['plot_src'] = plot_attachment['FileName']
                else:
                    product_dict['plot_src'] = f"cid:{plot_attachment['ContentId']}"

                jinja_payload['product_counts']['products'].append(product_dict)

            jinja_payload['product_counts']['products'] = [
                jinja_payload['product_counts']['products'][i:i+PRODUCT_COUNT_COLS] for
                i in range(0, len(jinja_payload['product_counts']['products']), PRODUCT_COUNT_COLS)
            ]
        except Exception as e:
            jinja_payload['product_counts']['error_count'] += 1
            jinja_payload['product_counts']['errors'].append(f'Failed to prepare report payload: {e}. '
                                                             f'Please report to PCM developer.')

        try:
            latencies_attachment = _get_attachment_by_filename(source_metadata['attachments'],
                                                                'opera_latency_query.png')

            if latencies_attachment is None:
                raise Exception('Cannot find latencies plot attachment')

            if bool(local_path):
                if isinstance(local_path, str) and os.path.isdir(local_path):
                    jinja_payload['latencies']['plot_src'] = os.path.join(local_path, latencies_attachment['FileName'])
                else:
                    jinja_payload['latencies']['plot_src'] = latencies_attachment['FileName']
            else:
                jinja_payload['latencies']['plot_src'] = f"cid:{latencies_attachment['ContentId']}"

        except Exception as e:
            jinja_payload['latencies']['error_count'] += 1
            jinja_payload['latencies']['errors'].append(f'Failed to prepare report payload: {e}. '
                                                             f'Please report to PCM developer.')

    # Product acc

    for key, value in _get_items_for_prefix(
        data,
        'ACCOUNTABILITY',
        exclude=('s1-slc',),
        sort=lambda x: (
            PRODUCT_ORDER.get(x.removeprefix('ACCOUNTABILITY-'), float('inf')),
            x
        )
    ):
        product_type = key.removeprefix('ACCOUNTABILITY-')

        product_acc_dict = {
            'product': product_type,
            'error_count': len(value['errors']),
            'errors': value['errors'],
            'points': [],
            'plots': [],
        }

        if not product_acc_dict['errors']:
            try:
                if product_type == 'DSWx-HLS':
                    product_acc_dict['points'].extend([
                        {
                            'key': 'Query start',
                            'value': value['data']['query_start_date'],
                            'flag': False
                        },
                        {
                            'key': 'Query end',
                            'value': value['data']['query_end_date'],
                            'flag': False
                        },
                        {
                            'key': 'Report run time',
                            'value': value['data']['report_run_time'],
                            'flag': False
                        },
                        {
                            'key': 'HLS Inputs',
                            'value': f"{value['data']['overall_counts']['hls_granules']:,}",
                            'flag': False
                        },
                        {
                            'key': 'Matched HLS Inputs',
                            'value': f"{value['data']['overall_counts']['matched_dswx_hls_granules']:,}",
                            'flag': False
                        },
                        {
                            'key': 'HLS to many DSWx',
                            'value': f"{value['data']['overall_counts']['hls_to_many_dswx']:,}",
                            'flag': value['data']['overall_counts']['hls_to_many_dswx'] > 0
                        },
                        {
                            'key': 'HLS to no DSWx',
                            'value': f"{value['data']['overall_counts']['hls_to_no_dswx']:,}",
                            'flag': value['data']['overall_counts']['hls_to_no_dswx'] > 0
                        },
                    ])
                elif product_type in ('CSLC-S1', 'RTC-S1'):
                    product_acc_dict['points'].extend([
                        {
                            'key': 'Query start',
                            'value': value['data']['query_start_time'],
                            'flag': False
                        },
                        {
                            'key': 'Query end',
                            'value': value['data']['query_end_time'],
                            'flag': False
                        },
                        {
                            'key': 'Expected burst count',
                            'value': f"{value['data']['expected_burst_count']:,}",
                            'flag': False
                        },
                        {
                            'key': 'Found burst count',
                            'value': f"{value['data']['found_burst_count']:,}",
                            'flag': False
                        },
                        {
                            'key': 'Missing burst count',
                            'value': f"{value['data']['missing_burst_count']:,}",
                            'flag': value['data']['missing_burst_count'] > 0
                        },
                        {
                            'key': 'Coverage percent',
                            'value': f"{value['data']['coverage_percent']}%",
                            'flag': value['data']['missing_burst_count'] > 0
                        },
                    ])
                elif product_type == 'DSWx-S1':
                    product_acc_dict['points'].extend([
                        {
                            'key': 'Query start',
                            'value': value['data']['query_start_time'],
                            'flag': False
                        },
                        {
                            'key': 'Query end',
                            'value': value['data']['query_end_time'],
                            'flag': False
                        },
                        {
                            'key': 'Total unused RTCs',
                            'value': f"{value['data']['total_unmapped_rtcs']:,}",
                            'flag': value['data']['total_reduced_native_id_count'] > 0
                        },
                        {
                            'key': 'Total unused RTCs (triggerable)',
                            'value': f"{value['data']['total_triggerable_unmapped_rtcs']:,}",
                            'flag': value['data']['total_reduced_native_id_count'] > 0
                        },
                        {
                            'key': 'Total affected tile sets',
                            'value': f"{value['data']['total_affected_tile_sets']:,}",
                            'flag': value['data']['total_reduced_native_id_count'] > 0
                        },
                        {
                            'key': 'Total reduced native ID count',
                            'value': f"{value['data']['total_reduced_native_id_count']:,}",
                            'flag': value['data']['total_reduced_native_id_count'] > 0
                        },
                        {
                            'key': 'Potential missing DSWx-S1 product count',
                            'value': f"{value['data']['potential_output_product_count']:,}",
                            'flag': value['data']['total_reduced_native_id_count'] > 0
                        },
                    ])
                else:
                    logger.error(f'Unexpected accountability product type will be ignored: {product_type}')

                plots = _get_attachments_by_content_type(value['attachments'], 'image/png')
                plots.sort(key=lambda x: x['FileName'])

                for plot in plots:
                    plot_dict = {}

                    if bool(local_path):
                        if isinstance(local_path, str) and os.path.isdir(local_path):
                            plot_dict['plot_src'] = os.path.join(local_path, plot['FileName'])
                        else:
                            plot_dict['plot_src'] = plot['FileName']
                    else:
                        plot_dict['plot_src'] = f"cid:{plot['ContentId']}"

                    product_acc_dict['plots'].append(plot_dict)
            except Exception as e:
                product_acc_dict['error_count'] += 1
                product_acc_dict['errors'].append(f'Failed to prepare accountability report payload for product {product_type}: {e}')

        jinja_payload['product_accountability'].append(product_acc_dict)

    # Duplicates

    for key, value in _get_items_for_prefix(
        data,
        'DUPLICATES',
        sort=lambda x: (
            PRODUCT_ORDER.get(x.removeprefix('DUPLICATES-'), float('inf')),
            x
        )
    ):
        product_type = key.removeprefix('DUPLICATES-')

        product_dup_dict = {
            'product': product_type,
            'error_count': len(value['errors']),
            'errors': value['errors'],
            'points': [],
            'plots': [],
        }

        if not product_dup_dict['errors']:
            try:
                product_dup_dict['points'].extend([
                    {
                        'key': 'Query start',
                        'value': value['data']['report_window_start_time'],
                        'flag': False
                    },
                    {
                        'key': 'Query end',
                        'value': value['data']['report_window_end_time'],
                        'flag': False
                    },
                    {
                        'key': 'Report run time',
                        'value': value['data']['report_run_time'],
                        'flag': False
                    },
                    {
                        'key': 'Total products',
                        'value': f"{value['data']['n_granules']:,}",
                        'flag':  False
                    },
                    {
                        'key': 'Total duplicates',
                        'value': f"{value['data']['n_duplicates']:,}",
                        'flag':  value['data']['n_duplicates'] > 0
                    },
                    {
                        'key': 'Percent duplicates',
                        'value': f"{value['data']['percent_duplicates']:.2f}%",
                        'flag':  value['data']['n_duplicates'] > 0
                    },
                ])

                if value['data']['n_duplicates'] > 0:
                    product_dup_dict['points'].extend([
                        {
                            'key': 'Min duplicates / granule',
                            'value': f"{value['data']['min_duplicates_per_granule']:,}",
                            'flag':  value['data']['n_duplicates'] > 0
                        },
                        {
                            'key': 'Max duplicates / granule',
                            'value': f"{value['data']['max_duplicates_per_granule']:,}",
                            'flag':  value['data']['n_duplicates'] > 0
                        },
                        {
                            'key': 'Average duplicates / granule',
                            'value': f"{value['data']['avg_duplicates_per_granule']:.2f}",
                            'flag':  value['data']['n_duplicates'] > 0
                        },
                    ])

                plots = _get_attachments_by_content_type(value['attachments'], 'image/png')
                plots.sort(key=lambda x: x['FileName'])

                for plot in plots:
                    plot_dict = {}

                    if bool(local_path):
                        if isinstance(local_path, str) and os.path.isdir(local_path):
                            plot_dict['plot_src'] = os.path.join(local_path, plot['FileName'])
                        else:
                            plot_dict['plot_src'] = plot['FileName']
                    else:
                        plot_dict['plot_src'] = f"cid:{plot['ContentId']}"

                    product_dup_dict['plots'].append(plot_dict)
            except Exception as e:
                product_dup_dict['error_count'] += 1
                product_dup_dict['errors'].append(f'Failed to prepare duplicate report payload for product {product_type}: {e}')

        jinja_payload['duplicates'].append(product_dup_dict)

    # DAAC acc

    source_metadata = data['DAAC_delivery_status']

    jinja_payload['daac_accountability']['error_count'] = len(source_metadata['errors'])
    jinja_payload['daac_accountability']['errors'] = source_metadata['errors']

    if not source_metadata['errors']:
        try:
            accountability = source_metadata['data']['accountability']

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
                        product_failure_dict['skipped'] = len(accountability[product_type][CNM_INGEST_FAILED]) - len(
                            product_failure_dict['products'])
                        product_failure_dict['skipped_str'] = f"{product_failure_dict['skipped']:,}"
                    else:
                        product_failure_dict['skipped'] = 0
                    html_ingestion_failures.append(product_failure_dict)

            jinja_payload['daac_accountability'].update({
                'rows': html_report_rows,
                'colors': {
                    1: 'yellow',
                    2: '#FFC000',
                    3: 'red',
                },
                # 'venue': venue,  # TODO: Get this?
                'start': source_metadata['data']['query']['start_time'],
                'end': source_metadata['data']['query']['end_time'],
                'ingestion_failures': html_ingestion_failures,
            })
        except Exception as e:
            jinja_payload['daac_accountability']['error_count'] += 1
            jinja_payload['daac_accountability']['errors'].append(f'Failed to prepare DAAC accountability report '
                                                                  f'payload: {e}. Please report to PCM developer.')

    # Render

    if debug:
        with open('debug_jinja_payload.json', 'w') as f:
            json.dump(jinja_payload, f, indent=2)

    template_directory = os.getcwd()
    template_filename = 'big_report_template.html.jinja2'

    template_loader = jinja2.FileSystemLoader(template_directory)

    template_env = jinja2.Environment(loader=template_loader,
                                      autoescape=jinja2.select_autoescape())

    template = template_env.get_template(template_filename)
    html_report_str = template.render(jinja_payload)

    if debug:
        with open('debug_big_report.html', 'w') as f:
            f.write(html_report_str)

    return html_report_str, jinja_payload


def _get_s3_from_dataset_urls(ds_urls):
    dataset_s3_bucket = None
    dataset_s3_prefix = None

    for ds_url in ds_urls:
        ds_url = urlparse(ds_url)
        if ds_url.scheme == 's3':
            if 'amazonaws.com' in ds_url.netloc:
                path = ds_url.path.lstrip('/')
                dataset_s3_bucket, dataset_s3_prefix = path.split('/', maxsplit=1)
            else:
                dataset_s3_bucket = ds_url.netloc
                dataset_s3_prefix = ds_url.path.lstrip('/')
            break

    if dataset_s3_bucket is None or dataset_s3_prefix is None:
        raise Exception('Could not find dataset s3 bucket and dataset s3 prefix')

    return dataset_s3_bucket, dataset_s3_prefix


def get_attachments(data):
    attachments = []

    for v in data['report_data'].values():
        attachments.extend(v.get('attachments', []))

    main_attachments = [a for a in attachments if a['ContentDisposition'] == 'ATTACHMENT']
    inline_attachments = [a for a in attachments if a['ContentDisposition'] == 'INLINE']

    return attachments, main_attachments, inline_attachments


def stage_attachments(attachments, bucket, prefix, path=None):
    for a in attachments:
        if path is not None:
            logger.info(f'Staging attachment {a["FileName"]} to {os.path.join(path, a['FileName'])}')
            s3.download_file(
                bucket,
                os.path.join(prefix, a['FileName']),
                os.path.join(path, a['FileName']),
            )
        else:
            logger.info(f'Retrieving attachment bytes for {a["FileName"]}')
            response = s3.get_object(
                Bucket=bucket,
                Key=os.path.join(prefix, a['FileName']),
            )
            a['RawContent'] = response['Body'].read()
            if 'ContentDescription' not in a:
                a['ContentDescription'] = a['FileName']
            a['ContentTransferEncoding'] = 'BASE64'

    return attachments


def lambda_handler(event, context):
    response = {}

    try:
        ses = boto3.client('sesv2')

        # Inject venue variable into the report metadata
        venue = os.getenv('VENUE', 'UNKNOWN')
        event['product_metadata']['venue'] = venue

        dataset_s3_bucket, dataset_s3_prefix = _get_s3_from_dataset_urls(event['dataset_urls'])

        _, main_attachments, inline_attachments = get_attachments(event['product_metadata'])

        logger.info(f'Loading and encoding {len(main_attachments)} main attachments')
        main_attachments = stage_attachments(
            main_attachments,
            dataset_s3_bucket,
            dataset_s3_prefix,
        )

        logger.info(f'Loading and encoding {len(inline_attachments)} inline attachments')
        inline_attachments = stage_attachments(
            inline_attachments,
            dataset_s3_bucket,
            dataset_s3_prefix,
        )

        attachment_size = (sum([len(a['RawContent']) for a in main_attachments]) +
                           sum([len(a['RawContent']) for a in inline_attachments]))

        logger.info(f'Attachments gross size: {attachment_size:,} bytes')

        if attachment_size > ATTACHMENT_SIZE_THRESHOLD:
            logger.warning(f'Gross attachment size over threshold of {ATTACHMENT_SIZE_THRESHOLD:,} bytes. Trying to zip.')
            buf = BytesIO()

            with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zip_f:
                for a in main_attachments:
                    zip_f.writestr(a['FileName'], a['RawContent'])
                    logger.info(f'Added {a["FileName"]}')

            attachments = inline_attachments + [{
                'RawContent': buf.getvalue(),
                'ContentType': 'application/zip',
                'ContentDisposition': 'ATTACHMENT',
                'FileName': 'supplementary_report_data.zip',
                'ContentTransferEncoding': 'BASE64',
            }]

            new_attachments_size = sum([len(a['RawContent']) for a in attachments])

            logger.info(f'Post-compression attachments gross size: {new_attachments_size:,} bytes')

            # if new_attachments_size > ATTACHMENT_SIZE_THRESHOLD:
            #     raise RuntimeError('TODO: post-zip attachments still too big! TODO: push to S3 instead?')
        else:
            attachments = main_attachments + inline_attachments

        html_report, meta = render_html_report(event['product_metadata'])

        logger.info(f'HTML report size: {len(html_report.encode("utf-8")):,} bytes')

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

        email_content = {
            'Simple': {
                'Subject': {
                    'Data': f"OPERA SDS Status Report [{meta['general']['tag'].upper()}] – "
                            f"{meta['general']['venue']} – Time: {meta['timestamp_report']}",
                },
                'Body': {
                    'Html': {
                        'Data': html_report,
                    }
                },
                'Attachments': attachments,
            }
        }

        ses_call = {
            'FromEmailAddress': sender,
            'Destination': dst,
            'ReplyToAddresses': [sender],
            'Content': email_content,
        }

        resp = None

        try:
            resp = ses.send_email(**ses_call)

            logger.info(f'SES response:')
            logger.info(json.dumps(resp, indent=2, default=str))

            response.update({
                'success': True,
                'ses_response': resp,
            })
        except Exception as e:
            logger.critical(e)
            if resp is not None:
                logger.info(f'\n{json.dumps(resp, indent=2, default=str)}')
            raise
    except Exception as e:
        import traceback

        response.update({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exception(e),
        })

    return response


def main_dev():
    with open('test_input_json.txt', 'r') as f:
        event = f.read()

    event = json.loads(event)

    cwd = os.getcwd()

    dataset_s3_bucket, dataset_s3_prefix = _get_s3_from_dataset_urls(event['dataset_urls'])

    stage_dir = os.path.join(cwd, 'test_data')

    if os.path.isdir(stage_dir):
        shutil.rmtree(stage_dir)

    os.makedirs(stage_dir)

    stage_attachments(
        get_attachments(event['product_metadata'])[0],
        dataset_s3_bucket,
        dataset_s3_prefix,
        stage_dir,
    )

    render_html_report(event['product_metadata'], debug=True, local_path=stage_dir)


if __name__ == '__main__':
    main_dev()
