from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from extract_raw_data import extract_raw_data_ftn
from data_cleansed import data_cleansed_ftn
from data_metrics import data_metrics_ftn

# define default arguments for the DAG
default_args = {
    'owner': 'julia',
    'start_date': datetime(2023, 3, 19),
}

#define the DAG with the specified arguments
dag = DAG(
    'dag_oetker_app',
    default_args=default_args,
    schedule_interval='@daily'
)

# task to run the extract_raw_data_ftn function
run_extract_raw_data = PythonOperator(
    task_id='run_my_script_task',
    python_callable=extract_raw_data_ftn,
    dag=dag
)

# # task to run the data_cleansed_ftn function
run_data_cleansed = PythonOperator(
    task_id='run_data_cleansed',
    python_callable=data_cleansed_ftn,
    dag=dag
)

# task to run the data_metrics_ftn function
run_data_metrics = PythonOperator(
    task_id='run_data_metrics',
    python_callable=data_metrics_ftn,
    dag=dag
)

run_extract_raw_data >> run_data_cleansed >> run_data_metrics
