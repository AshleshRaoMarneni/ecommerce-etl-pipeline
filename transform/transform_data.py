
import pandas as pd

def transform_sales(df):
    # Remove cancelled invoices
    df = df[~df["InvoiceNo"].astype(str).str.startswith("C")]

    # Remove negative quantities
    df = df[df["Quantity"] > 0]

    # Handle missing CustomerID
    df = df.dropna(subset=["CustomerID"])

    # Convert date
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])

    # Calculate revenue
    df["Revenue"] = df["Quantity"] * df["UnitPrice"]

    return df

if __name__ == "__main__":

    raw_df = pd.read_csv("data/processed/extracted_sales.csv", encoding="latin1")
    clean_df = transform_sales(raw_df)
    clean_df.to_csv("data/processed/extracted_sales.csv", index=False)

