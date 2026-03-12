import pandas as pd
import psycopg2
from io import StringIO
from data_processing import prepare_all_dataframes 

'''Create the tables using the data_processing.py in the database and load the data from 
the xlsx file into the tables.

For simplicity the database is created separately and the connection parameters are
added to a config.txt file that needs to be created with the user specific information
in the format:

'hostname','database','username','pwd','port_id' 

prepare_all_dataframes() is called in the main block to load and prepare the data from the xlsx file
before inserting it into the database. The function will return the prepared dataframes for each table, right
now supports:
- invoices
- customers
- products 
- product_sales
'''

def get_connection_details(config_file):
    '''Reads the connection details from a config file and returns them as a dictionary
    for direct input into the psycopg2.connect() function'''
    try:
        with open(config_file, 'r') as file:
            line = file.readline().strip()
            hostname, database, username, pwd, port_id = line.split(',')
            connection_details = {
                'host': hostname.strip(),
                'dbname': database.strip(),
                'user': username.strip(),
                'password': pwd.strip(),
                'port': port_id.strip()
            }
            return connection_details
    except FileNotFoundError:
            print(f"Error: The file '{config_file}' was not found.")
            return None
    except Exception as e:
            print(f"An error occurred while reading the config file: {e}")
            return None

def connect_to_database(connection_details):
    '''Establishes a connection to the PostgreSQL database using the provided connection details.
    ⚠ Warning: Always close the connection after use to prevent resource leaks.'''

    #always necessary to initialize the variable so the finally block can work outside this function
    conn = None

    try:
        conn = psycopg2.connect(**connection_details)
        print("Connection to the database was successful.\n")
        return conn
    except Exception as e:
        print(f"An error occurred while connecting to the database: {e}")
        return None
    
def create_tables(conn, file_name):
    '''Creates tables in the database using the provided connection and list of table names.'''
    cursor = None

    try:
        cursor = conn.cursor()
        with open(file_name, 'r') as file:
            sql_commands = file.read().split(';')  # Split the SQL commands by semicolon

            print(f"Found {len(sql_commands)} SQL statements to execute\n")

            for i, command in enumerate(sql_commands, 1):
                command = command.strip()
                if command:
                    try:
                        print(f"Executing statement {i}...")
                        print(f"  {command[:60]}...")  # Print first 60 chars
                        cursor.execute(command)
                        print(f"  ✓ Success")
                    except Exception as e:
                        print(f"  ✗ Failed: {e}")
                        raise  # Re-raise to trigger rollback
            
            conn.commit()
            print("\n✓ All tables created successfully.")

    except FileNotFoundError:
        print(f"Error: The file '{file_name}' was not found.")

    except Exception as e:
        print(f"An error occurred while creating tables: {e}")
        conn.rollback()

    finally:
        if cursor is not None:
            cursor.close()
            print("Cursor closed.")

def truncate_tables(conn, table_names):
    '''Truncates the specified tables in the database. Maintains
    the schema but removes all data.'''

    cursor = None

    try:
        cursor = conn.cursor()
        for table in table_names:
            try:
                print(f"Truncating table '{table}'...")
                cursor.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;")
                print(f"  ✓ Success")
            except Exception as e:
                print(f"  ✗ Failed: {e}")
                raise  # Re-raise to trigger rollback
        conn.commit()
        print("\n✓ All tables truncated successfully.")

    except Exception as e:
        print(f"An error occurred while truncating tables: {e}")
        conn.rollback()

    finally:
        if cursor is not None:
            cursor.close()
            print("Cursor closed.")

def get_table_names(conn):
    '''Retrieves the names of all tables in the database.'''

    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]
        return table_names
    except Exception as e:
        print(f"An error occurred while retrieving table names: {e}")
        return None
    finally:
        if cursor is not None:
            cursor.close()
            print("Cursor closed.")

def get_column_names(conn, table_name):
    '''Retrieves the column names of a specified table from a database.'''

    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(f"Select * FROM {table_name} LIMIT 0")
        column_names = [desc[0] for desc in cursor.description]
        return column_names
    except Exception as e:
        print(f"An error occurred while retrieving column names: {e}")
        raise
    except KeyError as e:
        print(f"Error: The table '{table_name}' does not exist. {e}")
        raise
    finally:
        if cursor is not None:
            cursor.close()
            print("Cursor closed.")

def insert_one_dataframe(dataFrame, conn, table_name):
    '''Insert DataFrame into table using tab-delimited format'''
    
    column_names = get_column_names(conn, table_name)
    
    if len(column_names) != len(dataFrame.columns):
        raise ValueError(f"Column count mismatch for table '{table_name}'")
    
    dataFrame = dataFrame[column_names].copy()
    
    print(f"Inserting {len(dataFrame)} rows into '{table_name}'...")
    
    # Use TAB as delimiter (less likely to appear in data)
    csv_buffer = StringIO()
    dataFrame.to_csv(
        csv_buffer, 
        index=False, 
        header=False,
        sep='\t',      # Tab delimiter
        na_rep='\\N'
    )
    csv_buffer.seek(0)
    
    cursor = conn.cursor()
    try:
        cursor.copy_from(
            csv_buffer, 
            table_name, 
            sep='\t',      # Match the delimiter
            null='\\N',
            columns=column_names
        )
        conn.commit()
        print(f"✓ Inserted {len(dataFrame)} rows into '{table_name}'")
    except Exception as e:
        conn.rollback()
        print(f"✗ Failed: {e}")
        raise
    finally:
        cursor.close()

def insert_all_dataframes(dataFrames, conn, table_names: list):
    '''Inserts data from multiple pandas DataFrames into their corresponding 
    tables in the database.'''
    try:
        for table in table_names:
            print(f"Inserting data into '{table}'...")
            insert_one_dataframe(dataFrames[table], conn, table)
    
    except Exception as e:
        print(f"An error occurred while inserting data: {e}")
        raise

def validate_data_loaded(list_of_dataFrames, conn, table_names):
    '''Validates that the data was loaded correctly by comparing row counts.
    TODO: Extend this function to compare sample data from the DataFrames with the database
    and check for duplicates and nulls in the database.'''
    
    cursor = None
    try:
        cursor = conn.cursor()

        for df, table in zip(list_of_dataFrames, table_names):
            cursor.execute(f"SELECT COUNT(*) FROM {table};")
            db_count = cursor.fetchone()[0]
            df_count = len(df)

            if db_count == df_count:
                print(f"✓ Validation passed for '{table}': {db_count} rows in database," +
                      f" {df_count} rows in DataFrame.")
            else:
                print(f"✗ Validation failed for '{table}': {db_count} rows in database," +
                      f" {df_count} rows in DataFrame.")
                raise ValueError(f"Row count mismatch for table '{table}'")
    except ValueError as e:
        print(f"Validation error: {e}")
        conn.rollback()
    finally:
        if cursor is not None:
            cursor.close()

def drop_tables(conn, table_names):
    '''Drops the specified tables from the database.'''
    cursor = None
    try:
        cursor = conn.cursor()
        for table in table_names:
            try:
                print(f"Dropping table '{table}'...")
                cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
                print(f"  ✓ Success")
            except Exception as e:
                print(f"  ✗ Failed: {e}")
                raise  # Re-raise to trigger rollback
        conn.commit()
        print("\n✓ All tables dropped successfully.")
    except Exception as e:
        print(f"An error occurred while dropping tables: {e}")
        conn.rollback()
    finally:        
        if cursor is not None:
            cursor.close()
            print("Cursor closed.")




if __name__ == "__main__":

    config_file = 'config.txt'
    data_file = 'Online Retail.xlsx'

    try:
        connection_details = get_connection_details(config_file)
        print(f"Connection details: {connection_details}\n")
        conn = connect_to_database(connection_details)

        # They have to be added in the right order to avoid foreign key constraint
        # errors when dropping and creating tables
        table_names = ['customers', 'products','invoices','product_sales']

        drop_tables(conn, table_names)
        create_tables(conn, 'sql/create_tables.sql')
        
        # get table names and column names for validation and 
        # loading data from DF into the tables
        
        print(f"Tables in the database: {get_table_names(conn)}\n")
        print(f"Tables inserted: {table_names}\n")

        for table in table_names:
            column_names = get_column_names(conn, table)
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
        
        insert_all_dataframes(data_frames, conn, table_names)

        
    except Exception as e:
        print(f"An error occurred: {e}")

    except FileNotFoundError as e:
        print(f"An error occurred: {e}")

    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")