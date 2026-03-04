import pandas as pd
import numpy as np
import psycopg2 


def calc_line_total(dataFrame):
    '''calculates the line total in this type of invoice info
    adds new column directly to the dataFrame
    - can be mouldable for differently named columns'''

    # Validate required columns
    if 'UnitPrice' not in dataFrame.columns:
        raise KeyError("Column 'UnitPrice' not found in DataFrame")
    if 'Quantity' not in dataFrame.columns:
        raise KeyError("Column 'Quantity' not found in DataFrame")
    
    # Check for nulls and warn
    null_count = (dataFrame['UnitPrice'].isnull() | dataFrame['Quantity'].isnull()).sum()
    if null_count > 0:
        print(f"⚠ Warning: {null_count} rows have null UnitPrice or Quantity")
        print(f"  line_total will be null for these rows")
    
    # Calculate (nulls will naturally propagate)
    dataFrame['line_total'] = dataFrame['Quantity'] * dataFrame['UnitPrice']

def group_information(dataFrame, group_by, column_to_aggregate, new_column_names, 
                      type_of_aggregations, drop_nulls=False, replace_nulls=False):
    '''Uses groupby to recover information from a given set of columns in a dataFrame and 
    saves them in a new dataFrame with the new column names 
    groups by the first column in dataFrame \n
    drop_nulls = True => Drops nulls for new tables that should not have them (e.g. customers)'''

    names_dict = {}

    if len(column_to_aggregate) != len(new_column_names) - 1:
        raise ValueError("Column to aggregate should have + 1 length than" + 
                         "\n new names lists (because of the group by column)")
    if len(column_to_aggregate) != len(type_of_aggregations):
        raise ValueError("Column to aggregate and type of aggregations lists must be same length")

    # pôr o id de guest em vez de null nos customers 
    if drop_nulls == True:
        dataFrame = dataFrame.dropna()


    for name, aggregation in zip(column_to_aggregate,type_of_aggregations):
        names_dict[name] = aggregation

    grouping = dataFrame.groupby(by=group_by, as_index=False).agg(names_dict)

    column_names = grouping.columns.tolist()

    for old_name, new_name in zip(column_names,new_column_names):   
        grouping.rename(columns = {old_name : new_name}, inplace=True)

    if replace_nulls == True and 'customer_id' in new_column_names:
        grouping.customer_id = grouping.customer_id.fillna(0)
        grouping.customer_id = grouping.customer_id.replace({"": 0})

    return grouping

def clean_id_column(dataFrame, id_column):
    '''removes the 'C' and 'A' prefixes from ID column'''
    
    if id_column not in dataFrame.columns:
        raise ValueError(f"Column '{id_column}' not found")
    
    dataFrame = dataFrame.copy()
    
    # Checking nulls, returns and other operations before cleaning
    # TODO: Later check for other prefixes using regex and report them
    nulls_before = dataFrame[id_column].isna().sum()
    print(f"ℹ Starting with {nulls_before} null values in {id_column}")

    count_C = dataFrame[id_column].str.startswith('C').sum()
    print(f'{count_C} returns found\n')

    count_A = dataFrame[id_column].str.startswith('A').sum()
    print(f'{count_A} other operations found\n')

    # Convert to string, handling nulls
    dataFrame[id_column] = dataFrame[id_column].fillna('').astype(str)
    
    # Remove prefixes
    dataFrame[id_column] = (dataFrame[id_column]
                            .str.replace('^C', '', regex=True)  # ^ means "start of string"
                            .str.replace('^A', '', regex=True))
    
    # Convert back to numeric
    dataFrame[id_column] = pd.to_numeric(dataFrame[id_column], errors='coerce')
    
    # Report and drop nulls
    nulls_after = dataFrame[id_column].isna().sum()
    if nulls_after > nulls_before:
        print(f"⚠ {nulls_after - nulls_before} IDs could not be converted to numbers")
    
    dataFrame = dataFrame.dropna(subset=[id_column])
    print(f"✓ Cleaned {id_column}: {len(dataFrame)} valid rows remaining\n")
    
    return dataFrame

def add_return_info(dataFrame, total_column):
    '''Adds a boolean column to the invoices table indicating if the invoice is a return or not'''
    
    if total_column not in dataFrame.columns:
        raise ValueError(f"Column '{total_column}' not found in DataFrame")
    
    dataFrame['is_return'] = dataFrame[total_column] < 0
    print(f"✓ Added 'is_return' column based on '{total_column}'\n")

def data_casting(dataFrame, column_name, new_type):
    '''casts a column to a new type, if possible'''

    if len(column_name) != len(new_type):
        raise ValueError("Column names and types lists must be same length")

    for column,dtype in zip(column_name, new_type):

        try:
            # check if column exists before casting
            if column not in dataFrame.columns:
                print(f"⚠ Warning: Column '{column}' not found in DataFrame. "+
                      "Skipping type casting for this column.")
                continue

            if 'int' in str(dtype).lower() and dataFrame[column].isna().any():
                nan_count = dataFrame[column].isna().sum()
                print(f"⚠ Column '{column}': {nan_count} NaN values, using nullable Int64")
                dataFrame[column] = dataFrame[column].astype('Int64')
            else:
                dataFrame[column] = dataFrame[column].astype(dtype)

        except ValueError as e:
            print(f"✗ Error casting column '{column}' to {dtype}: {e}")
            print(f"  Sample values: {dataFrame[column].head(3).tolist()}")
            raise  # Re-raise to stop execution (if absent, it would continue and be raised
            # on the outer try except block, leading to all kinds of errors down the line)

        except Exception as e:
            print(f"✗ Unexpected error with column '{column}': {type(e).__name__}: {e}")
            raise

def select_information(dataFrame, columns_to_extract, new_names):
    ''' for grouping columns from original DataFrame in a new table, without aggregation
    functions being applied'''

    if len(columns_to_extract) != len(new_names):
        raise ValueError("Columns to extract and new names lists must be same length")

    new_dataFrame = dataFrame[columns_to_extract]
    
    for i, name in zip(range(len(new_names)), columns_to_extract):
        new_dataFrame.rename(columns = {name : new_names[i]},inplace=True)

    return new_dataFrame

def validate_data(dataFrame, name):
    """Basic data quality checks"""
    print(f"\n{name} DataFrame:")
    print(f"  Shape: {dataFrame.shape}")
    print(f"  Nulls:\n{dataFrame.isnull().sum()}")
    print(f"  Duplicates: {dataFrame.duplicated().sum()}")

def load_data(file_name):
    '''loads the data from the excel file and returns a DataFrame
    with error handling'''
    
    try:
        dataFrame = pd.read_excel(file_name)
        print(f"✓ Successfully loaded {len(dataFrame)} rows from {file_name}")
        print(f"  Columns ({len(dataFrame.columns)}): {dataFrame.columns.tolist()}")
        return dataFrame
    except FileNotFoundError:
        print(f"Error: File {file_name} not found.")
        return None
    except Exception as e:
        print(f"An error occurred while loading the data: {type(e).__name__}: {e}")
        return None


def prepare_all_dataframes(file_name):
    '''orchestrator that returns all dataframes:
    - invoices
    - customers
    - products
    - product_sales'''

    try:
        # Load and initial inspection
        online_retail = load_data(file_name)
        if online_retail is None:
            print("Failed to load data. Exiting.")
            exit(1)

        # Clean Data
        print("\n===== CLEANING DATA =====")
        online_retail = clean_id_column(online_retail, 'InvoiceNo')
        online_retail['Description'] = online_retail['Description'].str.strip().str.upper()
        print("✓ Data cleaned\n")
        
        # Calculation before aggregation 
        calc_line_total(online_retail)

        # Create tables for the database
        print("===== CREATING TABLES =====")

        # define new column names for the tables to be created
        invoices_columns = ['invoice_no','invoice_date','customer_id',
                            'invoice_total', 'items_total']
        products_columns = ['stock_code','product_description']
        customers_columns = ['customer_id','country','first_purchase_date']
        product_sales_columns = ['invoice_no','stock_code','product_description',
                                'quantity','unit_price', 'line_total']

        # Create Tables
        invoices = group_information(online_retail, 'InvoiceNo',
                                    ['InvoiceDate', 'CustomerID', 'line_total','Quantity'],
                                    invoices_columns, ['first','first','sum','sum'], replace_nulls=True)
        print(f"✓ Invoices: {len(invoices)} rows")

        '''TODO: First purchase date needs to be adapted; what if the client is already 
        in the database?'''
        customers = group_information(online_retail,'CustomerID', 
                                    ['Country', 'InvoiceDate'], customers_columns, 
                                    ['first','min'], replace_nulls=True)
        print(f"✓ Customers: {len(customers)} rows")

        product_sales = select_information(online_retail, 
                                        ['InvoiceNo','StockCode','Description','Quantity','UnitPrice','line_total'],
                                        product_sales_columns)
        print(f"✓ Product Sales: {len(product_sales)} rows")
            
        product_sales['sale_id'] = range(1, len(product_sales) + 1)
        print(product_sales.columns.tolist())

        products = group_information(online_retail,'StockCode',
                                    ['Description'],products_columns,
                                    ['first'])
        print(f"✓ Products: {len(products)} rows")
            
        # Add additional info to tables
        add_return_info(invoices, 'invoice_total')

        # Type Casting Data
        print("===== TYPE CASTING =====")

        data_casting(invoices, invoices.columns.tolist(), 
                    ['int64','datetime64[ns]','int','float64','int64','boolean'])
        print("✓ Invoices typed")

        data_casting(customers, customers.columns.tolist(), 
                    ['int','str','datetime64[ns]'])
        print("✓ Customers typed")

        data_casting(product_sales, product_sales.columns.tolist(), 
                    ['int64','str', 'str', 'int64','float64','float64','int64'])
        print("✓ Product Sales typed")

        data_casting(products, products.columns.tolist(),
                    ['str','str'])
        print("✓ Products typed\n")

        # Remove possible duplicates in products table
        products = products.drop_duplicates(subset=['stock_code'])
        customers = customers.drop_duplicates(subset=['customer_id'])
        
        print(f"There are {invoices["invoice_no"].nunique()} unique invoices in invoices")
        print(f"There are {product_sales["invoice_no"].nunique()} unique invoices in product sales")


        # Call for each DataFrame
        print("===== FINAL VALIDATION =====")
        validate_data(invoices, "Invoices")
        validate_data(products, "Products")
        validate_data(customers, "Customers")
        validate_data(product_sales, "Product Sales")

        return {'invoices': invoices, 'customers': customers, 
            'products': products, 'product_sales': product_sales}

    except FileNotFoundError as e:
        print(f"\n✗✗✗ FATAL ERROR: {e}")
        print("Check that the file path is correct")
        exit(1)
        
    except ValueError as e:
        print(f"\n✗✗✗ DATA ERROR: {e}")
        print("Check data quality and function parameters")
        exit(1)
        
    except Exception as e:
        print(f"\n✗✗✗ UNEXPECTED ERROR: {type(e).__name__}")
        print(f"Message: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == '__main__':
    try:
        data_file = 'Online Retail.xlsx'

        dataframes = prepare_all_dataframes(data_file)

        invoices = dataframes['invoices']
        customers = dataframes['customers']
        products = dataframes['products']
        product_sales = dataframes['product_sales']

        if invoices is not None and customers is not None and products is not None and product_sales is not None:
            print("DataFrames loaded successfully.\n")
            print(f"'invoices': {invoices.shape[0]} rows, {invoices.shape[1]} columns")
            print(f"'customers': {customers.shape[0]} rows, {customers.shape[1]} columns")
            print(f"'products': {products.shape[0]} rows, {products.shape[1]} columns")
            print(f"'product_sales': {product_sales.shape[0]} rows, {product_sales.shape[1]} columns")

            print("\nSample data from 'invoices':")
            print(invoices.head(50))

        else:
            raise ValueError("One or more DataFrames failed to load. Please check the data processing step.")
        
    except FileNotFoundError as e:
        print(f"\n✗✗✗ FATAL ERROR: {e}")
        print("Check that the file path is correct")
        exit(1)
    except ValueError as e:
        print(f"\n✗✗✗ DATA ERROR: {e}")
        print("Check data quality and function parameters")
        exit(1)
    except Exception as e:
        print(f"\n✗✗✗ UNEXPECTED ERROR: {type(e).__name__}")
        print(f"Message: {e}")
        import traceback
        traceback.print_exc()
        exit(1)