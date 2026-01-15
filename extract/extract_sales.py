import pandas as pd
import os

# Path to your Excel file
raw_file_path = os.path.join("data", "raw", "Online Retail.xlsx")

def extract_sales():
    # Read the Excel file
    df = pd.read_excel(raw_file_path)
    print(f"Extracted {len(df)} rows from {raw_file_path}")
    
    # Save a copy in processed folder (optional first step)
    processed_path = os.path.join("data", "processed", "extracted_sales.csv")
    df.to_csv(processed_path, index=False)
    print(f"Saved extracted data to {processed_path}")
    
    return df

if __name__ == "__main__":
    extract_sales()

