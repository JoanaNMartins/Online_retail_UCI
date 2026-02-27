import pandas as pd
import numpy as np

online_retail = pd.read_excel('Online Retail.xlsx')
invoices = pd.DataFrame(columns=['invoice_id','invoice_date',
                                 'costumer_id','invoice_total',
                                 'items_total','is_return'])
products = pd.DataFrame(columns=['stock_code','product_description','current_price'])
costumers = pd.DataFrame(columns=['costumer_id','country'])
product_sales = pd.DataFrame(columns=['sale_id','invoice_id','stock_code','quantity','unit_price'])


online_retail['line_total'] = online_retail['Quantity'] * online_retail['UnitPrice']

print(online_retail.head())

invoice_grouping = online_retail.groupby(by='InvoiceNo', as_index=False).agg({
            'InvoiceDate': 'first',
            'CustomerID': 'first',
            'line_total': 'sum',
            'Quantity': 'sum'
        })

print(invoice_grouping.tail())