
CREATE DATABASE northwind

CREATE TABLE categories (
    category_id SMALLINT NOT NULL,
    category_name STRING NOT NULL,
    description STRING
);

CREATE TABLE customers (
    customer_id STRING NOT NULL,
    company_name STRING NOT NULL,
    contact_name STRING,
    contact_title STRING,
    address STRING,
    city STRING,
    region STRING,
    postal_code STRING,
    country STRING,
    phone STRING
);

CREATE TABLE employees (
    employee_id SMALLINT NOT NULL,
    last_name STRING NOT NULL,
    first_name STRING NOT NULL,
    title STRING,
    title_of_courtesy STRING,
    birth_date DATE,
    hire_date DATE,
    address STRING,
    city STRING,
    region STRING,
    postal_code STRING,
    country STRING,
    home_phone STRING,
    extension STRING,
    notes STRING,
    reports_to SMALLINT,
    photo_path STRING,
    salary FLOAT
);

CREATE TABLE order_details (
    order_id SMALLINT NOT NULL,
    product_id SMALLINT NOT NULL,
    unit_price FLOAT NOT NULL,
    quantity SMALLINT NOT NULL,
    discount FLOAT NOT NULL
);

CREATE TABLE orders (
    order_id SMALLINT NOT NULL,
    customer_id STRING,
    employee_id SMALLINT,
    order_date DATE ,
    required_date DATE,
    shipped_date DATE,
    ship_via SMALLINT,
    freight FLOAT,
    ship_name STRING,
    ship_address STRING,
    ship_city STRING,
    ship_region STRING,
    ship_postal_code STRING,
    ship_country STRING
)
CLUSTER BY (order_date);

CREATE TABLE products (
    product_id SMALLINT NOT NULL,
    product_name STRING NOT NULL,
    supplier_id SMALLINT,
    category_id SMALLINT,
    quantity_per_unit STRING,
    unit_price FLOAT,
    units_in_stock SMALLINT,
    units_on_order SMALLINT,
    reorder_level SMALLINT,
    discontinued BOOLEAN NOT NULL
);

CREATE TABLE shippers (
    shipper_id SMALLINT NOT NULL,
    company_name STRING NOT NULL,
    phone STRING
);

CREATE TABLE suppliers (
    supplier_id SMALLINT NOT NULL,
    company_name STRING NOT NULL,
    contact_name STRING,
    contact_title STRING,
    address STRING,
    city STRING,
    region STRING,
    postal_code STRING,
    country STRING,
    phone STRING,
    fax STRING,
    homepage STRING
);