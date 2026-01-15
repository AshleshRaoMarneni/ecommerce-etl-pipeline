import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns

engine = create_engine("postgresql+psycopg2://ecommerce_user:12345@localhost:5432/ecommerce_db")

# Read table
df = pd.read_sql("SELECT * FROM fact_sales", engine)

# Add a derived column
df['TotalPrice'] = df['Quantity'] * df['UnitPrice']

# Top 10 products by revenue
top_products = df.groupby('Description')['TotalPrice'].sum().sort_values(ascending=False).head(10)
top_products.plot(kind='bar', figsize=(10,6))
plt.title("Top 10 Products by Revenue")
plt.ylabel("Revenue")
plt.show()

