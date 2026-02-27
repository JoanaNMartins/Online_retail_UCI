import pandas as pd
import numpy as np
import psycopg2 


def calc_line_total(dataFrame):
    '''calculates the line total in this type of invoice info
    adds new column directly to the dataFrame
    - can be mouldable for differently named columns'''
    dataFrame['line_total'] = dataFrame['Quantity'] * dataFrame['UnitPrice']

def group_information(dataFrame, group_by, column_to_aggregate, new_column_names, 
                      type_of_aggregations, drop_nulls=False):
    '''Uses groupby to recover information from a given set of columns in a dataFrame and 
    saves them in a new dataFrame with the new column names 
    groups by the first column in dataFrame \n
    drop_nulls = True => Drops nulls for new tables that should not have them (e.g. customers)'''

    names_dict = {}

    if drop_nulls == True:
        dataFrame = dataFrame.dropna()

    for name, aggregation in zip(column_to_aggregate,type_of_aggregations):
        names_dict[name] = aggregation

    grouping = dataFrame.groupby(by=group_by, as_index=False).agg(names_dict)

    column_names = grouping.columns.tolist()

    for old_name, new_name in zip(column_names,new_column_names):   
        grouping.rename(columns = {old_name : new_name}, inplace=True)

    return grouping

def clean_id_column(dataFrame, id_column):
    '''removes the 'C' from the costumer id column and casts it to numeric, if possible'''
    dataFrame[id_column] = dataFrame[id_column].str.replace('C','')
    dataFrame[id_column] = dataFrame[id_column].str.replace('A','')
    dataFrame = dataFrame[dataFrame[id_column].notna()]
    dataFrame[id_column] = pd.to_numeric(dataFrame[id_column], errors='raise', downcast='integer')
    return dataFrame 

def add_return_info(dataFrame,total_column):
    '''adds return info True/False to the dataFrame, 
    based on the total column (if negative, it's a return)'''

    dataFrame['is_return'] = dataFrame[total_column] < 0


def select_information(dataFrame, columns_to_extract, new_names):
    ''' for grouping columns from original DataFrame in a new table, without aggregation
    functions being applied'''

    new_dataFrame = dataFrame[columns_to_extract]
    
    for i, name in zip(range(len(new_names)), columns_to_extract):
        new_dataFrame.rename(columns = {name : new_names[i]},inplace=True)

    return new_dataFrame

def data_casting(dataFrame, column_name, new_type):
    '''casts a column to a new type, if possible'''
    for column,type in zip(column_name, new_type):
        dataFrame[column] = dataFrame[column].astype(type)

def validate_data(dataFrame, name):
    """Basic data quality checks"""
    print(f"\n{name} DataFrame:")
    print(f"  Shape: {dataFrame.shape}")
    print(f"  Nulls:\n{dataFrame.isnull().sum()}")
    print(f"  Duplicates: {dataFrame.duplicated().sum()}")


if __name__ == '__main__':

    file_name = 'Online Retail.xlsx'

    # Load and initial inspection
    online_retail = pd.read_excel(file_name)
    print(f"Initial shape: {online_retail.shape}")
    print(f"Columns: {online_retail.columns.tolist()}")

    # define new column names for the tables to be created
    invoices_columns = ['invoice_id','invoice_date','customer_id',
                        'invoice_total', 'items_total']
    products_columns = ['stock_code','product_description']
    customers_columns = ['customer_id','country','first_purchase_date']
    product_sales_columns = ['invoice_id','stock_code','product_description','quantity','unit_price']

    # Clean Data
    online_retail = clean_id_column(online_retail, 'InvoiceNo')
    online_retail['Description'] = online_retail['Description'].str.strip().str.upper()

    # Calculation before aggregation 
    calc_line_total(online_retail)
    
    # Create tables for the database
    invoices = group_information(online_retail, 'InvoiceNo',
                                 ['InvoiceDate', 'CustomerID', 'line_total','Quantity'],
                                 invoices_columns, ['first','first','sum','sum'])

    '''TODO: First purchase date needs to be adapted; what if the client is already 
    in the database?'''
    customers = group_information(online_retail,'CustomerID', 
                                  ['Country', 'InvoiceDate'], customers_columns, 
                                  ['first','min'], drop_nulls=True)

    product_sales = select_information(online_retail, 
                                       ['InvoiceNo','StockCode','Description','Quantity','UnitPrice'],
                                       product_sales_columns)
    
    product_sales['sale_id'] = range(1, len(product_sales) + 1)

    products = group_information(online_retail,'StockCode',
                                 ['Description'],products_columns,
                                 ['first'])
    
    # Add additional info to tables
    add_return_info(invoices, 'invoice_total')

    # Type Casting Data
    data_casting(invoices, invoices.columns.tolist(), 
                 ['int64','datetime64[ns]','str','float64','int64','boolean'])
    data_casting(customers, customers.columns.tolist(), 
                 ['str','str','datetime64[ns]'])
    data_casting(product_sales, product_sales.columns.tolist(), 
                 ['int64','str', 'str', 'int64','float64','int64'])
    data_casting(products, products.columns.tolist(),
                 ['str','str'])

    # Remove possible duplicates in products table
    products = products.drop_duplicates(subset=['stock_code'])
    customers = customers.drop_duplicates(subset=['customer_id'])


    # Call for each DataFrame
    validate_data(invoices, "Invoices")
    validate_data(products, "Products")
    validate_data(customers, "Customers")
    validate_data(product_sales, "Product Sales")

