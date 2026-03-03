import pandas as pd
import psycopg2
import data_processing 

'''Create the tables using the data_processing.py in the database and load the data from 
the xlsx file into the tables.

For simplicity the database is created separately and the connection parameters are
added to a config.txt file that needs to be created with the user specific information
in the format:

hostname, database, username, pwd, port_id
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
                'hostname': hostname.strip(),
                'database': database.strip(),
                'username': username.strip(),
                'pwd': pwd.strip(),
                'port_id': port_id.strip()
            }
            return connection_details
    except FileNotFoundError:
            print(f"Error: The file '{config_file}' was not found.")
            return None
    except Exception as e:
            print(f"An error occurred while reading the config file: {e}")
            return None


if __name__ == "__main__":

    config_file = 'config.txt'

    connection_details = get_connection_details(config_file)

