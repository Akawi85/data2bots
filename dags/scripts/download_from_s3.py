# Download all the files using boto3
files_to_download = ['orders.csv','reviews.csv','shipment_deliveries.csv']

def download_raw(bucket, s3_object):
    for file in files_to_download:
        s3_object.download_file(bucket, f"orders_data/{file}", f"/opt/airflow/data/{file}")