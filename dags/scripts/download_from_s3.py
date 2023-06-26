import boto3
from botocore import UNSIGNED
from botocore.client import Config

s3_object = boto3.client('s3', config=Config(signature_version=UNSIGNED))
bucket_name='d2b-internal-assessment-bucket'

# Download all the files using boto3
files_to_download = ['orders.csv','reviews.csv','shipment_deliveries.csv']

def download_raw(bucket, s3_object):
    for file in files_to_download:
        s3_object.download_file(bucket, f"orders_data/{file}", f"/opt/airflow/data/{file}")
        print(f"Files downloaded to /opt/airflow/data/{file} successfully")

download_raw(bucket_name, s3_object)