import data_ingestion as di
import config as c
import decryption as dec


if __name__ =='__main__':
    db_config = c.Config.DB_URL
    di.create_tables(db_config)
    
    order_api = di.get_data_from_google_api(c.Config.ORDER_SHEET_ID, 'A1:P')
    review_api = di.get_data_from_google_api(c.Config.REVIEW_SPREADSHEET_ID,'A1:D')
    
    order_data = di.convert_to_dataframe(order_api)
    review_data = di.convert_to_dataframe(review_api)

    # di.column_conversion(order_data)
    # di.column_conversion(review_data)

    di.data_ingestion(order_data, 'orders_table', iteration_size=1000,retries=3)
    di.data_ingestion(review_data, "reviews_table", iteration_size=500, retries=2)