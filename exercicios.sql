// EXERCICIOS:

// SUMARIZA TOTAL DE VENDAS POR CATEGORIA

-- CATEGORIA 
-- PRODUTOS
-- ORDER_DETAILS [AQUI ESTAO OS VALORES]
CREATE OR REPLACE VIEW sales_by_categories AS
SELECT
    c.category_name     AS NM_CATEGORIA,
    SUM(od.unit_price)  AS VL_UNITARIO,
    SUM(od.quantity)    AS QTD,
    SUM(od.discount)    AS DESCONTO,
    (SUM(od.unit_price) * SUM(od.quantity)) - SUM(od.discount) AS VL_TOTAL
FROM CATEGORIES c
JOIN PRODUCTS p ON c.category_id = p.category_id
JOIN ORDER_DETAILS od ON p.product_id = od.product_id
GROUP BY c.category_name
;


SELECT * FROM sales_by_categories;

DROP VIEW IF EXISTS sales_by_categories ;

/* ****************************************************************************** */

// 3 PRODUTOS MAIS VENDIDOS DE CADA CATEGORIA

-- PRODUCTS
-- CATEGORIES
-- QUANTITY

WITH DATA_SOURCES 
    AS (
        SELECT
            c.category_name     AS NM_CATEGORIA,
            p.product_name      AS NM_PRODUTO,
            SUM(od.quantity)    AS QTD,
        FROM CATEGORIES c
        JOIN PRODUCTS p ON c.category_id = p.category_id
        JOIN ORDER_DETAILS od ON p.product_id = od.product_id
        GROUP BY c.category_name, p.product_name
),
RANKING
    AS (
        SELECT
            NM_PRODUTO,
            NM_CATEGORIA,
            QTD,
            RANK() OVER( PARTITION BY NM_CATEGORIA ORDER BY QTD ) AS RANKEAMENTO
        FROM DATA_SOURCES
),
FILTERING
    AS (
        SELECT 
            * 
        FROM RANKING
        WHERE RANKEAMENTO <= 3
        ORDER BY 
            NM_CATEGORIA,
            RANKEAMENTO
)
SELECT * FROM FILTERING ;
;
    
/* ****************************************************************************** */


//TOTAL e MEDIA VENDIDA DE VENDAS POR ANO e MES

-- PRODUCTS
-- CATEGORIES
-- QUANTITY


WITH DATA_SOURCES 
    AS (
        SELECT
            -- c.category_name     AS NM_CATEGORIA,
            -- p.product_name      AS NM_PRODUTO,
            EXTRACT(
                MONTH 
                FROM o.order_date
                )               AS MES,
            EXTRACT(
                YEAR 
                FROM o.order_date
                )               AS ANO,
            SUM(od.quantity)    AS QTD,
            AVG(od.quantity)    AS QTD_MEDIA,
        FROM CATEGORIES c
        JOIN PRODUCTS p         ON c.category_id = p.category_id
        JOIN ORDER_DETAILS od   ON p.product_id  = od.product_id
        JOIN ORDERS o            ON od.order_id   = o.order_id
        GROUP BY 
            -- c.category_name, 
            -- p.product_name,
            EXTRACT(
                MONTH 
                FROM o.order_date
                ),
            EXTRACT(
                YEAR 
                FROM o.order_date
                )
)
SELECT
    -- NM_PRODUTO,
    -- NM_CATEGORIA,
    ANO,
    MES,
    QTD,
    QTD_MEDIA
FROM DATA_SOURCES
ORDER BY
    -- NM_PRODUTO,
    -- NM_CATEGORIA,
    ANO,
    MES
;

/* ****************************************************************************** */

// AQUIVO parquet DO s3 PARA SNOWFLAKE

CREATE OR REPLACE TABLE employeespq (
    employee_id SMALLINT,
    last_name STRING,
    first_name STRING,
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

COPY INTO employeespq
FROM s3://snow-flake-udemy/employees.parquet
CREDENTIALS=(AWS_KEY_ID='AKIAS2OOWEIRPABYNY4Z' AWS_SECRET_KEY='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')
FILE_FORMAT=(TYPE='PARQUET')
MATCH_BY_COLUMN_NAME= CASE_INSENSITIVE
;

SELECT * FROM EMPLOYEESPQ LIMIT 5;