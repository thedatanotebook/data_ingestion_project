from config import Config
import pandas as pd
import sqlalchemy
import requests
from googleapiclient.discovery import build
from google.oauth2 import service_account
import psycopg2
from psycopg2 import sql
import os
import time

database_config = Config.DB_URL


def create_tables(database_config):
    """
    Creates the orders and reviews tables in a dataset.
    """
    try:
        conn = psycopg2.connect(database_config)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders_table (
                order_id varchar(255),
                order_date varchar(255),
                country varchar(255),
                state varchar(255),
                city varchar(255),
                region varchar(255),
                segment varchar(255),
                ship_mode varchar(255),
                category varchar(255),
                sub_category varchar(255),
                product varchar(255),
                discount varchar(255),
                sales varchar(255),
                profit varchar(255),
                quantity varchar(255),
                feedback varchar(255)
                );
            """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reviews_table (
            order_id varchar(255),
            order_date varchar(255),
            products varchar(255),
            ratings varchar(255)
            );
        """
        )
        conn.commit()
        print("Order and Reviews Table Created")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

def get_data_from_google_api(spreadsheet_id, range_name):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = service_account.Credentials.from_service_account_file(Config.XVRP, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', []) 
    print(f'Retrieved data from api.')
    return values

#Orders Table: 'A1:P'
#Reviews Table: 'A1:D'
    
# def data_ingestion(df, table_name, engine):
#     df.to_sql(table_name,con=engine, if_exists='append', index=False)
#     return("Completed")

def convert_to_dataframe(data):
    df = pd.DataFrame(data[1:], columns=data[0])
    df.fillna(0, inplace=True)
    return df

def column_conversion(df):
    df.columns.str.lower()
    df.columns.str.strip()
    df.columns.str.replace(" ", "_")

def data_ingestion(final_df, table_name, iteration_size: int, retries: int):
    engine = sqlalchemy.create_engine(Config.ALCHEMY_URL)
    
    # data_quality_check()

    for i in range(0, len(final_df), iteration_size):
        iteration = final_df.iloc[i:i+iteration_size]
        for retry in range(retries):
            start_time = time.time()
            try:
                iteration.to_sql(table_name, con=engine, if_exists='append', index=False)
                duration = time.time() - start_time
                print(f'Inserted {i // (iteration_size+1)} % for {duration}s for {table_name}')
                break
            except Exception as error:
                time.sleep(2**retry)
                print(error)


def data_quality_check(df):
    if df.isnull().values.any():
        raise ValueError("Found null values")
    elif not all(column in df.columns for column in ['order_id']):
        raise ValueError("Columns Missing Values")
    







# def ingest_data_frame(df, table_name, engine):
#     df.to_sql(table_name,con=engine, if_exists='append', index=False)
#     engine.dispose()

# def get_google_review_sheet_rows():
#     spreadsheet_id = Config.REVIEW_SPREADSHEET_ID
#     range_name = 'A1:D'  # Specify the range of cells
#     result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
#     values = result.get('values', []) 
#     return values

# def get_google_order_sheet_rows(): # Adjust scope as needed
#     spreadsheet_id = Config.ORDER_SHEET_ID
#     range_name = 'A1:P'  # Specify the range of cells
#     result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
#     values = result.get('values', []) 
#     return values

# def convert_google_api_to_dataframe(data):
#     df_api_data = pd.DataFrame(data[1:], columns=data[0])
#     return df_api_data 

# # Main 


