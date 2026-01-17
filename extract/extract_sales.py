import pandas as pd

# Load raw Excel
df = pd.read_excel("data/raw/raw_sales.xlsx")
# Standardize column names
df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]

# Save to processed layer
df.to_csv("data/processed/extracted_sales.csv", index=False)

