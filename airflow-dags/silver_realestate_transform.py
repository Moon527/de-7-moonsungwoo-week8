"""Q2: 같은 월의 Q1 성공을 기다린 뒤 spark-submit으로 Silver 정제를 실행한다."""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor


with DAG(
    dag_id='silver_realestate_transform',
    start_date=pendulum.datetime(2025, 1, 1, tz='Asia/Seoul'),
    end_date=pendulum.datetime(2025, 2, 1, tz='Asia/Seoul'),
    schedule='@monthly',
    catchup=True,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        'owner': 'moonsungwoo', 'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'execution_timeout': timedelta(minutes=30),
    },
    tags=['week8', 'Q2', 'moonsungwoo'],
    description='문성우: 실거래 자료 정제, UDF 두 개, 지역별 IQR, Silver Parquet',
) as dag:
    wait_for_bronze = ExternalTaskSensor(
        task_id='wait_for_bronze',
        external_dag_id='bronze_realestate_collect',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed'],
        check_existence=True,
        mode='reschedule',
        poke_interval=30,
        timeout=1800,
    )

    transform_silver = BashOperator(
        task_id='transform_silver',
        bash_command='''
set -euo pipefail
exec spark-submit --master 'local[2]' --driver-memory 1g \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.ui.port=4040 \
  --conf spark.port.maxRetries=0 \
  /opt/airflow/scripts/Q2/silver_spark.py \
  --month "$SILVER_MONTH" --ui-hold-seconds "$SILVER_UI_HOLD_SECONDS"
''',
        env={
            'PYTHONUNBUFFERED': '1',
            'PYSPARK_PYTHON': 'python3',
            'SILVER_MONTH': "{{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y%m') }}",
            # 기본값은 0초. 실습 캡처를 위해 설정했을 때만 첫 달 UI를 유지한다.
            'SILVER_UI_HOLD_SECONDS': "{{ var.value.get('week8_silver_ui_hold_seconds', '0') if data_interval_start.in_timezone('Asia/Seoul').strftime('%Y%m') == '202501' else '0' }}",
        },
        append_env=True,
        do_xcom_push=False,
    )

    wait_for_bronze >> transform_silver
