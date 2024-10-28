import os
from dotenv import load_dotenv
import argparse

# load_dotenv()

class Config():
    DB_URL = os.getenv('DB_URL')
    DATA_FILE_PATH = os.getenv('CSV_PATH')
    REVIEW_SPREADSHEET_ID = os.getenv('REVIEW_SHEET_ID')
    ORDER_SHEET_ID = os.getenv('ORDER_SHEET_ID')
    XVRP = os.getenv('XVRP')
    POSTGRES_USER=os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD=os.getenv('POSTGRES_PASSWORD')
    POSTGRES_HOST=os.getenv('POSTGRES_HOST')
    DB=os.getenv('DB')
    ALCHEMY_URL = os.getenv('ALCHEMY_URL')


