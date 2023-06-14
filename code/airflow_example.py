from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# set default args
default_args = {
    'owner': '',
    'start_date': datetime(2100, 1, 1, 0, 0),
    'schedule_interval': '@daily',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


def load_data():
    print('load data')


def process_data():
    print('process data')


def save_data():
    print('save_data')


with DAG('data_workflow', default_args=default_args) as dag:
    # define task
    load_data_task = PythonOperator(
        task_id='load_data_task',
        python_callable=load_data,
        provide_context=True

    )
    process_data_task = PythonOperator(
        task_id='process_data_task',
        python_callable=process_data,
        provide_context=True

    )
    save_data_task = PythonOperator(
        task_id='save_data_task',
        python_callable=save_data,
        provide_context=True

    )

# define workflow
load_data >> process_data >> save_data
