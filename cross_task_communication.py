import time
import pendulum
from datetime import datetime, timedelta


from airflow import DAG

from airflow.providers.standard.operators.python import PythonOperator




def increment_by_1(value):
    print("Value {value}".format(value=value))

    return value + 1


def multiply_by_100(ti):
    value = ti.xcom_pull(task_ids="increment_by_1")
    print("Value {value}!".format(value=value))

    return value * 100


def subtract_9(ti):
    value = ti.xcom_pull(task_ids="multiply_by_100")
    print("Value {value}!".format(value=value))

    return value - 9


def print_value(ti):
    value = ti.xcom_pull(task_ids="subtract_9")
    print("Value {value}".format(value=value))
    
default_args = {"owner": "Admin"}

with DAG(
    dag_id="cross_talk_communication",
    description="Cross_task communication with XCom",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 2, 21, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["xcom", "python"],
) as dag:
    increment_by_1 = PythonOperator(
        task_id="increment_by_1",
        python_callable=increment_by_1,
        op_kwargs={"value": 1},
    )
    multiply_by_100 = PythonOperator(
        task_id="multiply_by_100",
        python_callable=multiply_by_100,
    )
    subtract_9 = PythonOperator(task_id="subtract_9", python_callable=subtract_9)
    print_value = PythonOperator(task_id="print_value", python_callable=print_value)

increment_by_1 >> multiply_by_100 >> subtract_9 >> print_value
