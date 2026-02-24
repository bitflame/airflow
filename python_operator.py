import time
import pendulum

from airflow import DAG


from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    "owner": "admin",
}


def greet_hello(name):
    print("Hello, {name}!".format(name=name))

def greet_hello_with_city(name, city):
    print("Hello, {name} from {city}".format(name=name, city=city))


with DAG(
    dag_id="execut_python_operators",
    description="Python operators in DAGs with parameters",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 2, 18, tz="UTC"),
    schedule="@daily",
    tags=["Operators", "python"],
) as dag:

    taskA = PythonOperator(task_id="greet_hello", python_callable=greet_hello, op_kwargs={'name':'desmond'})
    taskB = PythonOperator(task_id="greet_hello_with_city", python_callable=greet_hello_with_city,op_kwargs={'name':'Louise', 'city':'Seattle'})

taskA >> taskB
