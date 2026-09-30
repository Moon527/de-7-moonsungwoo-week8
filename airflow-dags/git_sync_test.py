"""Q4: git-sync로 배포된 DAG를 BashOperator만으로 실행하여 확인한다."""
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="git_sync_test",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    default_args={"owner": "moonsungwoo", "retries": 0},
    tags=["week8", "Q4", "moonsungwoo"],
    description="문성우: Kubernetes Airflow의 git-sync 배포 및 실행 확인",
) as dag:
    check_deployment = BashOperator(
        task_id="check_deployment",
        bash_command="echo '문성우: GitHub에서 동기화한 테스트 DAG 실행 시작'",
    )

    print_time = BashOperator(
        task_id="print_time",
        bash_command="date -u '+실행 시각: %Y-%m-%d %H:%M:%S UTC'",
    )

    finish = BashOperator(
        task_id="finish",
        bash_command="echo '문성우: git-sync 테스트 DAG 실행 완료'",
    )

    check_deployment >> print_time >> finish
