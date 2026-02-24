from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    PythonOperator,
    BranchPythonOperator,
)
from random import choice
import pendulum

default_args = {"owner": "admin"}


def choose_branch():
    return choice([True, False])


def branch(ti):
    if ti.xcom_pull(task_ids="taskChoose"):
        return "taskC"
    else:
        return "taskD"


def task_c():
    print("TASK C executed!")


with DAG(
    dag_id="cron_catchup_backfill",
    description="Using crons, catchup, and backfill",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 2, 19, tz="UTC"),
    schedule_interval="0 0 * * *", # cron expression components: min, hour, Day of Month, Month, Year * means every 
    catchup=True,
) as dag:

    taskA = BashOperator(task_id="taskA", bash_command=True)

    taskChoose = PythonOperator(task_id="taskChoose", python_callable=choose_branch)

    taskBranch = BranchPythonOperator(task_id="taskBranch", python_callable=branch)

    taskC = PythonOperator(
        task_id = 'taskC',
        python_callable=task_c
    )

    taskD = BashOperator(
        task_id = 'taskD',
        bash_command='echo TASK D has executed!'
    )

    taskE = EmptyOperator(
        task_id = 'taskE',
    )

    taskA >> taskChoose >> taskBranch >> [taskC, taskE]

    taskC >> taskD 