
from sqlalchemy import create_engine
import pandas as pd

# Example Postgres connection
user = "your_user"
password = "your_password"
host = "localhost"
port = "5432"
database = "your_db"

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

# Load CSV
curated_dir = "data/curated/"
fact_sales = pd.read_csv(curated_dir + "fact_sales.csv", low_memory=False)

# Write to Postgres
fact_sales.to_sql('fact_sales', engine, if_exists='replace', index=False)

