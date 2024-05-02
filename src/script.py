import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv 
import psycopg2
import uuid
import os
load_dotenv()

# --- Connect to the Database ---
DATABASE_TYPE = os.getenv('DATABASE_TYPE')
DBAPI = os.getenv('DBAPI')
HOST = os.getenv('HOST')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
DATABASE = os.getenv('DATABASE')
PORT = os.getenv('PORT')

# Create the database engine
engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

# Remove all the tables from the database
with engine.connect() as con:
    query = """DROP TABLE IF EXISTS "DimTime", "DimProduct", "DimCustomer", "DimGeography", "FactSales" CASCADE;"""
    con.execute(text(query))
    print("All tables dropped successfully.")

# Load the dataset
print("Loading dataset...")
df = pd.read_csv('../data/data.csv', encoding='ISO-8859-1')

# --- Transform the Data ---
print("Creating time table...")
# Convert InvoiceDate to datetime
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

# Generate a unique UUID for each unique InvoiceDate
unique_dates = df['InvoiceDate'].drop_duplicates()
date_uuid_map = {date: uuid.uuid4() for date in unique_dates}

# Apply this mapping to create TimeID for DimTime
df['TimeID'] = df['InvoiceDate'].map(date_uuid_map)

# Now create time_table with this mapping applied
time_table = pd.DataFrame({
    'TimeID': df['TimeID'].drop_duplicates(),
    'Date': [dt.date() for dt in unique_dates],
    'Month': [dt.month for dt in unique_dates],
    'Quarter': [dt.quarter for dt in unique_dates],
    'Year': [dt.year for dt in unique_dates],
    'Weekday': [dt.weekday() for dt in unique_dates],
    'Hour': [dt.hour for dt in unique_dates],
}).set_index('TimeID')

# Create the DimProduct table data
product_table = df[['StockCode', 'Description']].drop_duplicates(subset='StockCode').set_index('StockCode')

# Create the DimCustomer table data
customer_table = df[['CustomerID', 'Country']].drop_duplicates().set_index('CustomerID')

# Create the DimGeography table data
geography_table = df[['Country']].drop_duplicates()
geography_table['GeoID'] = range(1, len(geography_table) + 1)
geography_table = geography_table.set_index('GeoID')

# Create the FactSales table data
df['TotalPrice'] = df['Quantity'] * df['UnitPrice']
sales_table = df[['InvoiceNo', 'StockCode', 'CustomerID', 'TimeID', 'Quantity', 'UnitPrice', 'TotalPrice']].copy()
sales_table['SalesID'] = range(1, len(sales_table) + 1)
sales_table = sales_table.set_index('SalesID')


# --- Load the Data into the Database ---
print("Loading data into the database...")
try:
    # Load the time_table
    time_table.to_sql('DimTime', engine, if_exists='append')

    # Load the product_table
    product_table.to_sql('DimProduct', engine, if_exists='append')

    # Load the customer_table
    customer_table.to_sql('DimCustomer', engine, if_exists='append')

    # Load the geography_table
    geography_table.to_sql('DimGeography', engine, if_exists='append')

    # Load the sales_table
    sales_table.to_sql('FactSales', engine, if_exists='append')

    print('ETL process completed successfully!')
except Exception as e:
    print(e)
    print('ETL process failed.')
