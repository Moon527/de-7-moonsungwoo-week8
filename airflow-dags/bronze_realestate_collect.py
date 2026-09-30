"""
8주차 Q1: 서울 6개 지역의 아파트 매매 실거래 자료를 월별로 수집한다.

실습 수집 기간은 2025년 1~2월로 설정했다.
API 응답 XML은 원본 그대로 저장한다.
응답 건수가 전체 건수와 다르면 누락을 방지하기 위해 실패 처리한다.
인증키는 환경변수에서 읽는다.
"""
import hashlib
import os
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode, unquote
from urllib.request import urlopen
import xml.etree.ElementTree as ET

import boto3
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup

DISTRICTS = ('11680', '11650', '11710', '11440', '11170', '11200')
ENDPOINT = 'https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade'


def collect(lawd_cd, **context):
    print(f"collector=문성우, time={datetime.now(pendulum.timezone('Asia/Seoul'))}, lawd={lawd_cd}")
    month = context['data_interval_start'].in_timezone('Asia/Seoul').strftime('%Y%m')
    key = os.environ.get('REALESTATE_API_KEY', '').strip()
    if not key:
        raise AirflowException('REALESTATE_API_KEY is missing')
    query = urlencode(dict(serviceKey=unquote(key), LAWD_CD=lawd_cd,
                           DEAL_YMD=month, pageNo=1, numOfRows=10000))
    try:
        with urlopen(ENDPOINT + '?' + query, timeout=60) as response:
            payload = response.read()
    except Exception as exc:
        # Never include a URL containing the API key in an exception message.
        raise AirflowException('API request failed: ' + type(exc).__name__) from None
    try:
        root = ET.fromstring(payload)
    except ET.ParseError:
        print('XML parse error: routing to skip')
        return dict(status='parse_error', month=month)
    code = root.findtext('.//resultCode', default='MISSING')
    if code not in ('000', '00'):
        raise AirflowException('API resultCode=' + code)
    items = root.findall('.//item')
    try:
        total = int(root.findtext('.//totalCount', default=''))
    except ValueError:
        return dict(status='parse_error', month=month)
    if total == 0 and not items:
        print('No transactions: routing to skip')
        return dict(status='empty', month=month)
    if total != len(items):
        raise AirflowException(f'Incomplete response: total={total}, received={len(items)}; pagination required')
    run_hash = hashlib.sha256(context['run_id'].encode()).hexdigest()[:16]
    path = Path('/opt/airflow/data/bronze_staging') / run_hash / month / f'{lawd_cd}.xml'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    print(f'Collected month={month}, LAWD_CD={lawd_cd}, rows={total}')
    return dict(status='ready', month=month, path=str(path), rows=total)


def choose(lawd_cd, **context):
    result = context['ti'].xcom_pull(task_ids=f'district_{lawd_cd}.collect')
    target = 'upload' if result['status'] == 'ready' else 'skip'
    return f'district_{lawd_cd}.{target}'


def upload(lawd_cd, **context):
    result = context['ti'].xcom_pull(task_ids=f'district_{lawd_cd}.collect')
    bucket = os.environ.get('REALESTATE_BUCKET', '').strip()
    if not bucket:
        raise AirflowException('REALESTATE_BUCKET is missing')
    key = f"bronze/{result['month']}/{lawd_cd}.xml"
    # Upload the unchanged response bytes, not a reconstructed XML document.
    boto3.client('s3').put_object(
        Bucket=bucket, Key=key, Body=Path(result['path']).read_bytes(),
        ContentType='application/xml',
    )
    print(f"Uploaded s3://{bucket}/{key}, rows={result['rows']}")


with DAG(
    dag_id='bronze_realestate_collect',
    start_date=pendulum.datetime(2025, 1, 1, tz='Asia/Seoul'),
    end_date=pendulum.datetime(2025, 2, 1, tz='Asia/Seoul'),
    schedule='@monthly',
    catchup=True,
    max_active_runs=1,
    max_active_tasks=6,
    default_args={'owner': 'moonsungwoo', 'retries': 2,
                  'retry_delay': timedelta(minutes=1),
                  'execution_timeout': timedelta(minutes=10)},
    tags=['week8', 'Q1', 'moonsungwoo'],
    description='문성우: six districts, monthly raw XML collection',
) as dag:
    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')
    for lawd in DISTRICTS:
        with TaskGroup(group_id=f'district_{lawd}') as group:
            collect_task = PythonOperator(task_id='collect', python_callable=collect,
                                          op_kwargs={'lawd_cd': lawd})
            branch_task = BranchPythonOperator(task_id='branch', python_callable=choose,
                                               op_kwargs={'lawd_cd': lawd})
            upload_task = PythonOperator(task_id='upload', python_callable=upload,
                                         op_kwargs={'lawd_cd': lawd})
            skip_task = EmptyOperator(task_id='skip')
            done = EmptyOperator(task_id='done', trigger_rule='none_failed_min_one_success')
            collect_task >> branch_task >> [upload_task, skip_task]
            upload_task >> done
            skip_task >> done
        start >> group >> finish
