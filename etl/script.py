import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv 
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

# Load the dataset
df = pd.read_csv('data\data.csv')

# --- Transform the Data ---

# Convert InvoiceDate to datetime
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

# Create the DimTime table data
dim_time = df['InvoiceDate'].dt
time_table = pd.DataFrame({
    'TimeID': dim_time.strftime('%Y%m%d%H').astype(int),  # Combining date and hour for a unique TimeID
    'Date': dim_time.date,
    'Month': dim_time.month,
    'Quarter': dim_time.quarter,
    'Year': dim_time.year,
    'Weekday': dim_time.weekday,
    'Hour': dim_time.hour
}).drop_duplicates().set_index('TimeID')

# Create the DimProduct table data
product_table = df[['StockCode', 'Description']].drop_duplicates().set_index('StockCode')

# Create the DimCustomer table data
# Assuming the customer's first name and last name are not provided, using CustomerID
customer_table = df[['CustomerID', 'Country']].drop_duplicates().set_index('CustomerID')

# Create the DimGeography table data
# Assuming that only the Country information is available
geography_table = df[['Country']].drop_duplicates()
geography_table['GeoID'] = range(1, len(geography_table) + 1)
geography_table = geography_table.set_index('GeoID')

# Create the FactSales table data
# Assuming that the CSV has a TotalPrice column; if not, uncomment the next line to calculate it
# df['TotalPrice'] = df['Quantity'] * df['UnitPrice']
sales_table = df[['InvoiceNo', 'StockCode', 'CustomerID', 'InvoiceDate', 'Quantity', 'UnitPrice', 'TotalPrice']].copy()
sales_table['TimeID'] = sales_table['InvoiceDate'].dt.strftime('%Y%m%d%H').astype(int)
sales_table = sales_table.drop('InvoiceDate', axis=1)
sales_table['SalesID'] = range(1, len(sales_table) + 1)
sales_table = sales_table.set_index('SalesID')

# --- Load the Data into the Database ---

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
