
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import os

curated_dir = "/Users/adityaraomarneni/ecommerce-etl-pipeline/data/curated/"

# Load CSV
fact_sales = pd.read_csv(os.path.join(curated_dir, "fact_sales.csv"), low_memory=False)

# Use Airflow connection
hook = PostgresHook(postgres_conn_id='postgres_default')
engine = hook.get_sqlalchemy_engine()

# Write to Postgres
fact_sales.to_sql('fact_sales', engine, if_exists='replace', index=False)


