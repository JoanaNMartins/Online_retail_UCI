CREATE TABLE IF NOT EXISTS public.customers (
    customer_id INT PRIMARY KEY,
    country VARCHAR(255),
    first_purchase_date DATE
);

CREATE TABLE IF NOT EXISTS public.products (
    stock_code VARCHAR(255) PRIMARY KEY,
    description VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS public.invoices (
    invoice_no VARCHAR(255) PRIMARY KEY,
    invoice_date TIMESTAMP,
    customer_id INT,
    invoice_total DECIMAL,
    is_return BOOLEAN,
    items_total INT,
    FOREIGN KEY (customer_id) REFERENCES public.customers(customer_id)
);

CREATE TABLE IF NOT EXISTS public.product_sales (
    invoice_no VARCHAR(255),
    stock_code VARCHAR(255),
    product_description VARCHAR(255),
    quantity INT,
    unit_price DECIMAL,
    line_total DECIMAL,
    PRIMARY KEY (invoice_no, stock_code),
    FOREIGN KEY (invoice_no) REFERENCES public.invoices(invoice_no),
    FOREIGN KEY (stock_code) REFERENCES public.products(stock_code)
);


ALTER TABLE public.invoices OWNER TO postgres;
ALTER TABLE public.customers OWNER TO postgres;
ALTER TABLE public.products OWNER TO postgres;
ALTER TABLE public.product_sales OWNER TO postgres;

CREATE INDEX idx_customer_id ON public.invoices(customer_id);