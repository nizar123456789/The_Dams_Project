from airflow import DAG
import time 
import requests 
from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

from datetime import datetime

import requests



def print_welcome():

    print('Welcome to Airflow!')



def print_date():

    print('Today is {}'.format(datetime.today().date()))



def print_random_quote(retries=3, delay=5):
    for i in range(retries):
        try:
            response = requests.get('https://api.quotable.io/random')
            response.raise_for_status()
            quote = response.json()['content']
            print(f'Quote of the day: "{quote}"')
            return
        except requests.exceptions.RequestException as e:
            print(f"Attempt {i+1} failed: {e}")
            if i < retries - 1:
                time.sleep(delay)
            else:
                print("Failed to retrieve the quote after multiple attempts.")

dag = DAG(

    'Hello_dag',

    default_args={'start_date': days_ago(1)},

    schedule_interval='0 23 * * *',

    catchup=False

)



print_welcome_task = PythonOperator(

    task_id='print_welcome',

    python_callable=print_welcome,

    dag=dag

)



print_date_task = PythonOperator(

    task_id='print_date',

    python_callable=print_date,

    dag=dag

)



print_random_quote = PythonOperator(

    task_id='print_random_quote',

    python_callable=print_random_quote,

    dag=dag

)



# Set the dependencies between the tasks

print_welcome_task >> print_date_task >> print_random_quote

