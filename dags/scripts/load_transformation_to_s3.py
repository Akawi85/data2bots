import pandas as pd
import os

# download the transformed tables
transformed_path = '/opt/airflow/transformed'

def download_transformed(analytics_schema, connection_engine):
    transformed_tables_names = ['best_performing_product', 'agg_public_holiday', 'agg_shipments']
    for table in transformed_tables_names:
        df_transformed = pd.read_sql_query(f'select * from {analytics_schema}.{table}', con=connection_engine)
        df_transformed.to_csv(f'{transformed_path}/{table}.csv', index=False)

def upload_transformed_to_s3(s3_object, bucket):
    # Upload to s3
    for file in os.listdir(transformed_path):
        if file.endswith(".csv"):
            full_file_path = f"{transformed_path}/{file}"
            s3_object.upload_file(full_file_path, bucket, f'analytics_export/ifeaakaw4441/{file}')