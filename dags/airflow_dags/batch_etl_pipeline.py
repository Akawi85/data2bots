from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from datetime import timedelta
import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from scripts.download_from_s3 import download_raw
from scripts.load_to_postgres_staging import write_raw
from scripts.execute_transformation_scripts import execute_transform
from scripts.load_transformation_to_s3 import download_transformed, upload_transformed_to_s3

load_dotenv()

# driver=Variable.get('POSTGRES_DRIVER')
# host=Variable.get('POSTGRES_HOST')
# port=Variable.get('POSTGRES_PORT')
# database=Variable.get('POSTGRES_DATABASE')
# username=Variable.get('POSTGRES_USERNAME')
# password=Variable.get('POSTGRES_PASSWORD')
# staging_schema=Variable.get('POSTGRES_STAGING_SCHEMA')
# analytics_schema=Variable.get('POSTGRES_ANALYTICS_SCHEMA')
# bucket_name=Variable.get('S3_BUCKET')

driver='postgresql+psycopg2'
host='34.89.230.185'
port=5432
database='d2b_accessment'
username='ifeaakaw4441'
password='OcCwxlTINF'
staging_schema='ifeaakaw4441_staging'
analytics_schema='ifeaakaw4441_analytics'
bucket_name='d2b-internal-assessment-bucket'

# create engine that connects to the database
engine = create_engine(f'{driver}://{username}:{password}@{host}:{port}/{database}')
engine.connect()
print('Engine connected successfully!')

# create cursor object
connection = psycopg2.connect(database=database,
                        host=host,
                        user=username,
                        password=password,
                        port=port
                             )
connection.autocommit = True
cursor = connection.cursor()
print('Cursor object created')

# bucket_name = "d2b-internal-assessment-bucket"
s3_object = boto3.client('s3', config=Config(signature_version=UNSIGNED))
response = s3_object.list_objects(Bucket=bucket_name, Prefix="orders_data")

default_args={
    'owner':'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

# Instantiate the DAG with an ID and assign to a variable
ETL_dag = DAG(dag_id='ETL_batch_pipeline', 
                default_args=default_args,
                description='ETL Pipeline to pull from s3, load to postgres staging, transform staging data, pull transformed data and load transformed data to s3',
                schedule="*/10 * * * *",
                start_date=days_ago(1),
                catchup=False,
                tags=['ETL', 'BATCH', 'Data2Bots'],
    )

task_1 = PythonOperator(
    task_id='download_from_s3',
    python_callable = download_raw,
    op_args = [bucket_name, s3_object],
    dag=ETL_dag,
)

task_2 = PythonOperator(
    task_id='write_raw_to_postgres',
    python_callable = write_raw,
    op_args = [staging_schema, engine],
    dag=ETL_dag,
)

task_3 = PythonOperator(
    task_id='execute_transformation_scripts',
    python_callable = execute_transform,
    op_args = [cursor, analytics_schema],
    dag=ETL_dag,
)

task_3 = PythonOperator(
    task_id='execute_transformation_scripts',
    python_callable = execute_transform,
    op_args = [cursor, analytics_schema],
    dag=ETL_dag,
)

task_4 = PythonOperator(
    task_id='download_transformed_data',
    python_callable = download_transformed,
    op_args = [analytics_schema, engine],
    dag=ETL_dag,
)

task_5 = PythonOperator(
    task_id='upload_transformed',
    python_callable = upload_transformed_to_s3,
    op_args = [s3_object, bucket_name],
    dag=ETL_dag,
)

task_1 >> task_2 >> task_3 >> task_4 >> task_5
