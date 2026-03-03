import pandas as pd
import psycopg2
# import data_processing 

'''Create the tables using the data_processing.py in the database and load the data from 
the xlsx file into the tables.

For simplicity the database is created separately and the connection parameters are
added to a config.txt file that needs to be created with the user specific information
in the format:

'hostname','database','username','pwd','port_id' 
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
    '''Truncates the specified tables in the database.'''
    pass

def insert_dataframe():
    '''Inserts data from a pandas DataFrame into a specified table in the database.'''
    pass

def validate_data_loaded():
    '''Validates that the data was loaded correctly by comparing row counts or checksums.'''
    pass

def drop_tables(conn, table_names):
    '''Drops the specified tables from the database.'''
    pass

if __name__ == "__main__":

    config_file = 'config.txt'

    try:
        connection_details = get_connection_details(config_file)
        print(f"Connection details: {connection_details}\n")
        conn = connect_to_database(connection_details)
        create_tables(conn, 'sql/create_tables.sql')

    except Exception as e:
        print(f"An error occurred: {e}")

    except FileNotFoundError as e:
        print(f"An error occurred: {e}")

    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")