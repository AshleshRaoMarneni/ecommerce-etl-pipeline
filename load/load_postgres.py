

import pandas as pd
from sqlalchemy import create_engine

# Read your transformed CSV
df = pd.read_csv("data/processed/extracted_sales.csv")

# Connect to PostgreSQL
engine = create_engine(
    "postgresql+psycopg2://ecommerce_user:12345@localhost:5432/ecommerce_db"
)

# Load the data into a table called fact_sales
df.to_sql("fact_sales", engine, if_exists="replace", index=False)

print("Data loaded successfully!")

