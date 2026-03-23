SELECT 
    invoices.invoice_no,
    invoices.operation_type,
    products.product_description,
    product_sales.quantity,
    product_sales.stock_code
FROM product_sales
LEFT JOIN invoices ON invoices.invoice_no = product_sales.invoice_no
LEFT JOIN products ON products.stock_code = product_sales.stock_code
WHERE quantity < 0
ORDER BY invoice_no ASC
LIMIT 100
