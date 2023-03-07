from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime,timedelta
import sys

# Import the functions from the files

import thisisatest


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


with DAG('random_test2', default_args=default_args, schedule='0 19 27 * *') as dag:
    
    # Define the tasks for the DAG
    extract_task = PythonOperator(
        task_id='extract_data-1',
        python_callable=thisisatest.create_table
    )
    
    extract_task

dag 

'''




####################################################
test dag

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


with DAG('spotify_pipeline', default_args=default_args, schedule='0 19 27 * *') as dag:
    
    # Define the tasks for the DAG
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=Load.create_acx_table
    )
    
    extract_task

dag 

####################################################











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
    
    # Set the order of the tasks
    extract_task >> transform_task >> load_task >> execute_views_task


















from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from datetime import datetime,timedelta



default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id='axx',
    default_args=default_args, 
    start_date= datetime(2023, 2, 27),
    schedule='0 0 * * *'
) as dag:
    task1 = PostgresOperator(
        task_id = 'cacs',
        postgres_conn_id = 'postgres_conn',
        sql="""create table if not exists sadd (
                
                abc date
            
            );"""
    )


'''