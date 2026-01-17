from sqlalchemy import create_engine
from urllib.parse import quote_plus

user = 'postgres'
password = 'Happydays@123'   # raw password
host = 'localhost'
port = '5432'
database = 'retail_db'

# Encode password
password_enc = quote_plus(password)

engine = create_engine(f'postgresql+psycopg2://{user}:{password_enc}@{host}:{port}/{database}')

# Test connection
try:
    conn = engine.connect()
    print("Connected to Postgres successfully!")
    conn.close()
except Exception as e:
    print("Connection failed:", e)

