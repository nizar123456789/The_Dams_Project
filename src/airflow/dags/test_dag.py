from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from airflow.utils.dates import days_ago 
from datetime import datetime
from datetime import timedelta
from pipeline.data_ingestion import ingest_data
from pipeline.data_transformation import transform_data
import os 
import pandas as pd
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..','..' ,'pipeline')))

default_args={
  
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}
    
    
# Transform function: Simulate data transformation and save locally
def run_ingest_data(**kwargs):
    folder_path = r"C:\Users\nizar\Desktop\Dams_Project\src\pipeline\PDF_Downloads"
    data = ingest_data(folder_path)
    # Store the ingested data in XCom for downstream tasks
    kwargs['ti'].xcom_push(key='ingested_data', value=data.to_json())

# Define the Python function for the transform_data task
def run_transform_data(**kwargs):
    # Retrieve the data from XCom
    ingested_data = kwargs['ti'].xcom_pull(key='ingested_data', task_ids='ingest_data_task')
    # Convert the data back to a DataFrame
    data = pd.read_json(ingested_data)
    
    # Perform data transformation
    transformed_data = transform_data(data)
    
    # Store the transformed data in XCom or save it locally
    transformed_data.to_csv(r'C:\Users\nizar\Desktop\Dams_Project\output\transformed_data.csv', index=False)
    print('Data transformation complete, and file saved locally.')



dag = DAG(
    'testing_my_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval=timedelta(days=1),
)
# Define the task for ingesting data
ingest_data_task = PythonOperator(
    task_id='ingest_data_task',
    python_callable=run_ingest_data,
    provide_context=True,
    dag=dag,
)

# Define the task for transforming data
transform_data_task = PythonOperator(
    task_id='transform_data_task',
    python_callable=run_transform_data,
    provide_context=True,
    dag=dag,
)

ingest_data_task >> transform_data_task