"""Q3: 같은 월의 Silver 성공 대기 → Spark 집계·PostgreSQL 적재 → 건수 검증."""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor


MONTH = "{{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y%m') }}"

with DAG(
    dag_id='gold_realestate_aggregate',
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
    tags=['week8', 'Q3', 'moonsungwoo'],
    description='문성우: Gold 집계 5종, PostgreSQL 적재 및 건수 검증',
) as dag:
    wait_for_silver = ExternalTaskSensor(
        task_id='wait_for_silver',
        external_dag_id='silver_realestate_transform',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed'],
        check_existence=True,
        mode='reschedule',
        poke_interval=30,
        timeout=1800,
    )

    aggregate_gold = BashOperator(
        task_id='aggregate_gold',
        bash_command='''
set -euo pipefail
exec spark-submit --master 'local[2]' --driver-memory 1g \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.ui.port=4040 \
  /opt/airflow/scripts/Q3/gold_spark_sql.py --month "$GOLD_MONTH"
''',
        env={'GOLD_MONTH': MONTH, 'PYTHONUNBUFFERED': '1', 'PYSPARK_PYTHON': 'python3'},
        append_env=True,
        do_xcom_push=False,
    )

    # 이 task는 DB 건수만 검사한다. 위 Spark 집계 task는 반드시 spark-submit을 사용한다.
    verify_gold = BashOperator(
        task_id='verify_gold',
        bash_command='python /opt/airflow/scripts/Q3/gold_spark_sql.py --month "$GOLD_MONTH" --verify-only',
        env={'GOLD_MONTH': MONTH, 'PYTHONUNBUFFERED': '1'},
        append_env=True,
        do_xcom_push=False,
    )

    wait_for_silver >> aggregate_gold >> verify_gold
