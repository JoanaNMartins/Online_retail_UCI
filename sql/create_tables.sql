CREATE TABLE IF NOT EXISTS public.customers (
    customer_id INT PRIMARY KEY,
    country VARCHAR(255),
    first_purchase_date DATE
);

CREATE TABLE IF NOT EXISTS public.products (
    stock_code VARCHAR(255) PRIMARY KEY,
    product_description VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS public.invoices (
    invoice_no INT PRIMARY KEY,
    invoice_date TIMESTAMP,
    customer_id INT NULL,
    invoice_total DECIMAL(10, 2),
    items_total INT,
    FOREIGN KEY (customer_id) REFERENCES public.customers(customer_id)
);

CREATE TABLE IF NOT EXISTS public.product_sales (
    sale_id INT PRIMARY KEY,
    invoice_no INT,
    stock_code VARCHAR(255),
    quantity INT,
    unit_price DECIMAL(10, 2),
    line_total DECIMAL(10, 2),
    operation_type VARCHAR(20),
    FOREIGN KEY (invoice_no) REFERENCES public.invoices(invoice_no),
    FOREIGN KEY (stock_code) REFERENCES public.products(stock_code)
);


ALTER TABLE public.invoices OWNER TO postgres;
ALTER TABLE public.customers OWNER TO postgres;
ALTER TABLE public.products OWNER TO postgres;
ALTER TABLE public.product_sales OWNER TO postgres;

CREATE INDEX idx_customer_id ON public.invoices(customer_id);
CREATE INDEX idx_stock_code ON public.product_sales(stock_code);
CREATE INDEX idx_invoice_no ON public.product_sales(invoice_no);