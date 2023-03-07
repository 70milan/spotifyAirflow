from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime,timedelta
import sys
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timezone': 'US/Central',
}

def my_function():
    from theetl import extract_spotify_liked_songs
    extract_spotify_liked_songs()

def my_function0():
    from theetl import dataframes_transform
    dataframes_transform() 

def my_function1():
    from theetl import load_to_psgdb
    load_to_psgdb()   

def my_function2():
    from theetl import execute_views
    execute_views()   

with DAG('spl', default_args=default_args, schedule='0 19 27 * *',) as dag:
        # Define the tasks for the DAG
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=my_function,
        dag=dag
    )
    transform_task = PythonOperator(
        task_id='transform_spotify_data',
        python_callable=my_function0,
        dag=dag
    )
    load_task = PythonOperator(
        task_id='load_to_psgdb',
        python_callable=my_function1,
        dag=dag
    )
    extract_task >> transform_task  >> load_task 

'''


transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=l.dataframes_transform,
        op_kwargs={'e': e},
    )
    
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=l.load_to_psgdb,
        op_kwargs={
            'df_original': l.df_original,
            'df_date': l.df_date,
            'df_artists_final': l.df_artists_final,
            'df_unique_genres': l.df_unique_genres,
            'df_features': l.df_features
        },
    )
    
    execute_views_task = PythonOperator(
        task_id='execute_views',
        python_callable=l.execute_views,
        op_kwargs={'sql_file_path': Variable.get('sql_file_path')},
    )




>> transform_task >> load_task >> execute_views_task






import datetime

now = datetime.datetime.now()
day_name = now.strftime("%A")

print(day_name)  # Output: Wednesday


for 




import datetime

# Assigning index 1-7 to Sunday to Saturday
weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday','Sunday']

# Get the index of today's day of the week (0 = Sunday, 1 = Monday, ..., 6 = Saturday)
today_index = datetime.datetime.today().weekday()
today_index = 1

# Calculate the number of days until the next Friday
if today_index < 4:  # If today is not Friday
    days_until_friday = 4 - today_index
    print(days_until_friday, "day(s) till next Friday")
else:  # If today is Friday
    days_until_friday = 7 - (today_index-4 )
    print(days_until_friday, "day(s) till next Friday")

    '''