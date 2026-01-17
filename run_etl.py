import os
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# --------------------------
# Postgres Connection Setup
# --------------------------
user = 'postgres'
password = 'Happydays@123'  # your password
host = 'localhost'
port = '5432'
database = 'retail_db'

# URL-encode password for special characters
password_enc = quote_plus(password)
engine = create_engine(f'postgresql+psycopg2://{user}:{password_enc}@{host}:{port}/{database}')

# --------------------------
# Paths
# --------------------------
raw_file = "data/raw/Online Retail.xlsx"
processed_file = "data/processed/extracted_sales.csv"
curated_dir = "data/curated/"
os.makedirs(curated_dir, exist_ok=True)

# --------------------------
# 1️⃣ Extract
# --------------------------
print("Starting Extraction...")
df_raw = pd.read_excel(raw_file)
df_raw.to_csv(processed_file, index=False)
print("Extraction Completed.")

# --------------------------
# 2️⃣ Transform
# --------------------------
print("Starting Transformation...")
df = pd.read_csv(processed_file)

# Step 1: Standardize column names
df.columns = df.columns.str.strip().str.lower()
print("Columns after standardization:", df.columns.tolist())

# Step 2: Basic cleaning
df = df.dropna(subset=['invoiceno', 'stockcode'])  # remove rows with missing keys
df = df[df['quantity'] > 0]                         # remove rows with non-positive quantity
df = df.drop_duplicates(subset=['invoiceno', 'stockcode'])

# Step 3: Add revenue column
df['revenue'] = df['quantity'] * df['unitprice']

# Step 4: Create curated tables
# Fact Table
fact_sales = df[['invoiceno','invoicedate','stockcode','quantity','revenue']]
fact_sales.to_csv(os.path.join(curated_dir,"fact_sales.csv"), index=False)

# Product Dimension
dim_product = df[['stockcode','description','unitprice']].drop_duplicates()
dim_product.to_csv(os.path.join(curated_dir,"dim_product.csv"), index=False)

# Date Dimension
df['invoicedate'] = pd.to_datetime(df['invoicedate'], errors='coerce')
dim_date = pd.DataFrame({
    'date': df['invoicedate'].dt.date,
    'day': df['invoicedate'].dt.day,
    'month': df['invoicedate'].dt.month,
    'year': df['invoicedate'].dt.year
}).drop_duplicates()
dim_date.to_csv(os.path.join(curated_dir,"dim_date.csv"), index=False)

print("Transformation Completed.")

# --------------------------
# 3️⃣ Load
# --------------------------
print("Starting Load into Postgres...")
fact_sales.to_sql('fact_sales', engine, if_exists='replace', index=False)
dim_product.to_sql('dim_product', engine, if_exists='replace', index=False)
dim_date.to_sql('dim_date', engine, if_exists='replace', index=False)
print("Load Completed Successfully!")

print("\nETL Pipeline Finished Successfully!")

