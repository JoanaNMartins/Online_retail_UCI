from data_processing import prepare_all_dataframes
import database as db

def run_etl(config_file, data_file, table_names, 
            create_tables_file=None, create_tables=True, 
            drop_existing_tables=False):
    '''Function that orchestrates all the steps of the ETL process: 
    data preparation and database insertion.'''

    try:
        #Step 1: Setting up the connection 
        connection_details = db.get_connection_details(config_file)
        print(f"Connection details: {connection_details}\n")
        conn = db.connect_to_database(connection_details)

        if drop_existing_tables:
            db.drop_tables(conn, table_names)
        if create_tables:
            db.create_tables(conn, create_tables_file)

        # Step 2: Check table names and columns and 
        #         prepare all dataframes
        
        print(f"Tables in the database: {db.get_table_names(conn)}\n")
        print(f"Tables inserted: {table_names}\n")

        if sorted(db.get_table_names(conn)) != sorted(table_names):
            raise ValueError("The tables in the database do not match the expected table names."+
                             "Please check the database setup.")

        for table in table_names:
            column_names = db.get_column_names(conn, table)
            print(f"Columns in '{table}': {column_names}\n")
        
        data_frames = prepare_all_dataframes(data_file)
        invoices = data_frames['invoices']
        customers = data_frames['customers']
        products = data_frames['products']
        product_sales = data_frames['product_sales']

        if invoices is not None and customers is not None and products is not None and product_sales is not None:
            print("DataFrames loaded successfully.\n")
            print(f"'invoices': {invoices.shape[0]} rows, {invoices.shape[1]} columns")
            print(f"'customers': {customers.shape[0]} rows, {customers.shape[1]} columns")
            print(f"'products': {products.shape[0]} rows, {products.shape[1]} columns")
            print(f"'product_sales': {product_sales.shape[0]} rows, {product_sales.shape[1]} columns")
        else:
            raise ValueError("One or more DataFrames failed to load. Please check the data processing step.")

        # Step 3: Insert data into the database
        db.insert_all_dataframes(data_frames, conn, table_names)

        # Step 4: Verify the number of records inserted
        db.validate_data_loaded(data_frames, conn)

        print("\n✓✓✓ ETL PIPELINE COMPLETED SUCCESSFULLY ✓✓✓")
        
    except Exception as e:
        print(f"\n✗✗✗ ETL FAILED: {e}")
        raise  # Re-raise to show full stack trace
        
    finally:        
        if conn is not None:
            conn.close()
            print("\n✓ Database connection closed.")

def truncate_and_reload(config_file, data_file, table_names, create_tables_file):
    '''Function that truncates the tables and reloads the data.
    Does not drop and recreate the tables, so it is faster than the full ETL process.
    It is useful for refreshing the data without changing the database structure.'''

    try:
        connection_details = db.get_connection_details(config_file)
        conn = db.connect_to_database(connection_details)

        db.truncate_tables(conn, table_names)




        run_etl(config_file, data_file, table_names, create_tables_file, 
                create_tables=False, drop_existing_tables=False)

    except Exception as e:
        print(f"\n✗✗✗ TRUNCATE AND RELOAD FAILED: {e}")
        raise
    finally:
        if conn is not None:
            conn.close()
            print("\n✓ Database connection closed.")

if __name__ == "__main__":
    #running the full ETL process

    config_file = 'config.txt'
    data_file = 'Online Retail.xlsx'
    table_names = ['customers', 'products','invoices','product_sales']
    create_tables_file = 'sql/create_tables.sql'

    run_etl(config_file, data_file, table_names, 
            create_tables_file,drop_existing_tables=True)
    
