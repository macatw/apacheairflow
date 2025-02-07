from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import csv


def extract_data(ti):
    # Connect to MongoDB
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pymongo"])
    from pymongo import MongoClient
    client = MongoClient("mongodb.default", 27017)
    db = client['salesweek3airflow']
    sales_db = db['sales']
    try:
        data = pd.read_csv('/opt/airflow/dags/sales.csv')
    except FileNotFoundError as e:
        raise
    except Exception as e:
        raise
    try:
        # Load cleaned patient_data
        for index, row in data.iterrows():
            sales_db.insert_one(dict(row))
    except Exception as e:
        raise


# Define default arguments for the DAG
default_args = {
'owner': 'airflow',
'start_date': datetime(2023, 1, 1),
'retries': 1,
}
# Define the DAG
dag = DAG(
'etl_pipeline_mongodb',
default_args=default_args,
schedule_interval='0 6 * * *', # Run every day at 6:00 AM
)

# Create tasks
extract_task = PythonOperator(task_id='extract',python_callable=extract_data,dag=dag)
