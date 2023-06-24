import os

def read_sql_scripts(path, cursor, analytics_schema):
    with open(path, 'r') as file_handle:
        cursor.execute(file_handle.read())
        print(f"Performed Transformations and created {path.split('/')[-1].split('.')[0]} table in {analytics_schema}")

def execute_transform(cursor, analytics_schema): 
    scripts_path='/opt/airflow/sql_scripts'
    for file in os.listdir(scripts_path):
        if file.endswith(".sql"):
            full_file_path = f"{scripts_path}/{file}"
            read_sql_scripts(full_file_path, cursor, analytics_schema)