# transform/transform_data.py
import pandas as pd
import os

processed_file = "data/processed/extracted_sales.csv"
curated_dir = "data/curated/"
os.makedirs(curated_dir, exist_ok=True)

# Load processed CSV
df = pd.read_csv(processed_file)

# Basic cleaning
df = df.dropna(subset=['invoiceno', 'stockcode'])  # remove rows missing invoice or product
df = df[df['quantity'] > 0]                        # remove zero or negative quantity
df = df.drop_duplicates(subset=['invoiceno', 'stockcode'])

# Add revenue column
df['revenue'] = df['quantity'] * df['unitprice']

# Fact Table
fact_sales = df[['invoiceno','invoicedate','stockcode','quantity','revenue']]
fact_sales.to_csv(os.path.join(curated_dir,"fact_sales.csv"), index=False)
print("Fact table created")

# Product Dimension
dim_product = df[['stockcode','description','unitprice']].drop_duplicates()
dim_product.to_csv(os.path.join(curated_dir,"dim_product.csv"), index=False)
print("Product dimension table created")

# Date Dimension
df['invoicedate'] = pd.to_datetime(df['invoicedate'], errors='coerce')
dim_date = pd.DataFrame({
    'date': df['invoicedate'].dt.date,
    'day': df['invoicedate'].dt.day,
    'month': df['invoicedate'].dt.month,
    'year': df['invoicedate'].dt.year
}).drop_duplicates()
dim_date.to_csv(os.path.join(curated_dir,"dim_date.csv"), index=False)
print("Date dimension table created")

print("\nCurated Layer Completed Successfully!")



