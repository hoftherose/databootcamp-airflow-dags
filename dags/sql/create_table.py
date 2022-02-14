"""SQL command definitions"""

CREATE_USER_PURCHASE_TABLE = """
    DROP TABLE IF EXISTS public.user_purchase;
    CREATE TABLE IF NOT EXISTS public.user_purchase (
        invoice_number varchar(10),
        stock_code varchar(20),
        detail varchar(1000),
        quantity int,
        invoice_date timestamp,
        unit_price numeric(8,3),
        customer_id int,
        country varchar(20)
    );
"""
