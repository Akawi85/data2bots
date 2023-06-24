import pandas as pd

# Read the downloaded files into pandas DataFrame
# orders_df = pd.read_csv('/opt/airflow/data/orders.csv')
# reviews_df = pd.read_csv('/opt/airflow/data/reviews.csv')
# shipment_deliveries_df = pd.read_csv('/opt/airflow/data/shipment_deliveries.csv')

# def write_raw(staging_schema, connection_engine):
#     # write orders raw data to staging database
#     orders_df.to_sql(name='orders', schema=staging_schema, con=connection_engine, if_exists='replace', index=False) 
#     print(f"Loaded raw orders data into {staging_schema} successfully!")

#     # write reviews raw data to staging database
#     reviews_df.to_sql(name='reviews', schema=staging_schema, con=connection_engine, if_exists='replace', index=False) 
#     print(f"Loaded raw reviews data into {staging_schema} successfully!")

#     # write shipment_deliveries raw data to staging database
#     shipment_deliveries_df.to_sql(name='shipment_deliveries', schema=staging_schema, con=connection_engine, if_exists='replace', index=False) 
#     print(f"Loaded raw shipment_deliveries data into {staging_schema} successfully!")
print('hello')