CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer VARCHAR(255) NOT NULL,
    order_date DATE NOT NULL DEFAULT CURRENT_DATE,
    amount NUMERIC(10, 2) NOT NULL
);

CREATE VIEW public.orders_formatted_vw AS 
SELECT 
    id,
    customer,
    amount,
    to_char(order_date, 'YYYY-MM-DD') AS order_date
FROM public.orders;

INSERT INTO orders (customer, order_date, amount)
SELECT 
    (ARRAY['Alice Smith', 'Bob Jones', 'Charlie Brown', 'Diana Prince', 'Evan Wright', 'Fiona Gallagher', 'George Clark', 'Hannah Abbot', 'Ian Malcolm', 'Julia Roberts'])[i] AS customer,
    (CURRENT_DATE - (i || ' days')::INTERVAL)::DATE AS order_date,
    ROUND((RANDOM() * 450 + 50)::NUMERIC, 2) AS amount
FROM generate_series(1, 10) AS i;