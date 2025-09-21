
// BANCO DE DADOS OLTP REPLICADO NO DW NO SCHEMA RAW
-- customers
-- sales
-- products


// TRANSFORMACOES PARA CRIAR DM_VENDAS
-- fc_vendas
-- dm_clientes
-- dm_produtos
-- dm_tempo


// ABORDAGEM
-- SCD 2 
-- NOVOS REGISTRO: INSERT
-- ATUALIZAÇÃO: INSERT
-- SK GERADA AUTOMATICAMENTE
-- * ABSTRACOES:
--      - NAO HAVERA VALIDADE OU STATUS DAS DIMENSOES
--      - NAO HAVERA IMPLEMENTACAO DE "is_deleted" OU SEMELHANTES


// ETAPAS
-- customers_raw [OLTP] -> customers_stream -> SQL TASK -> dm_clientes
-- products_raw [OLTP] -> products_stream -> SQL TASK -> dm_produtos
-- SQL TASK -> dm_tempo
-- sales_raw [OLTP] -> sales_stream -> SQL TASK -> fc_vendas

-- CRIAR SCHEMA RAW e DATA_MART_VENDAS
-- DDL PARA RAW e DATA_MART_VENDAS
-- POPULAR 'dm_tempo'
-- CREATE STREAM
-- INSERT INTO NAS TABELAS DO DATA_MART_VENDAS
-- TESTAR
-- AUTOMATIZAR COM TASKs


/* ***************************************************************************** */

-- CRIAR BANCO DE DADOS
CREATE DATABASE IF NOT EXISTS prj_db_oltp_vendas
;


-- SELECIONANDO O BANCO PARA CRIAR OS OBJETOS
USE prj_db_oltp_vendas
;


-- CRIADNO SCHEMA 'raw'
CREATE SCHEMA IF NOT EXISTS prj_db_oltp_vendas.raw
;


-- CRIADNO SCHEMA 'data_mart_vendas'
CREATE SCHEMA IF NOT EXISTS prj_db_oltp_vendas.data_mart_vendas
;

CREATE SCHEMA IF NOT EXISTS prj_db_oltp_vendas.intermediate_vendas
;

/* ----------------------------------------------------------------------------- */

// SCRIPT 'raw'
DROP TABLE IF EXISTS sales;

CREATE TABLE IF NOT EXISTS customers_raw (
  customer_id INTEGER PRIMARY KEY,
  customer_name VARCHAR(50) NOT NULL,
  email VARCHAR(50) UNIQUE,
  address VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS products_raw (
  product_id INTEGER PRIMARY KEY,
  product_name VARCHAR(50) NOT NULL,
  description VARCHAR(500),
  price DECIMAL(10,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS sales_raw (
  sale_id INTEGER PRIMARY KEY,
  customer_id INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  sale_date DATE NOT NULL,
  quantity INTEGER NOT NULL,
  FOREIGN KEY (customer_id) REFERENCES customers_raw(customer_id),
  FOREIGN KEY (product_id) REFERENCES products_raw(product_id)
);


/* ----------------------------------------------------------------------------- */

// SCRIPT 'data_mart_vendas'

DROP TABLE IF EXISTS dm_tempo;

CREATE TABLE IF NOT EXISTS dm_clientes (
  customer_sk INTEGER AUTOINCREMENT PRIMARY KEY,
  customer_id INTEGER NOT NULL UNIQUE,
  customer_name VARCHAR(50) NOT NULL,
  email VARCHAR(50) UNIQUE,
  address VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dm_produtos (
  product_sk INTEGER AUTOINCREMENT PRIMARY KEY,
  product_id INTEGER NOT NULL UNIQUE,
  product_name VARCHAR(50) NOT NULL,
  description VARCHAR(500),
  price DECIMAL(10,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS dm_tempo (
  date_sk INTEGER AUTOINCREMENT PRIMARY KEY,
  date DATE NOT NULL UNIQUE,
  day INTEGER NOT NULL,
  month INTEGER NOT NULL,
  year INTEGER NOT NULL,
  quarter INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fc_vendas (
  sale_sk INTEGER AUTOINCREMENT PRIMARY KEY,	
  sale_id INTEGER,
  customer_sk INTEGER NOT NULL,
  product_sk INTEGER NOT NULL,
  date_sk INTEGER NOT NULL,
  quantity INTEGER NOT NULL,
  FOREIGN KEY (customer_sk) REFERENCES dm_clientes(customer_sk),
  FOREIGN KEY (product_sk) REFERENCES dm_produtos(product_sk),
  FOREIGN KEY (date_sk) REFERENCES dm_tempo(date_sk)
);


/* ----------------------------------------------------------------------------- */

-- INSERT NA DIMENSAO 'dm_tempo' DO SCHEMA 'data_mart_vendas'
INSERT  INTO dm_tempo ( DATE,DAY,MONTH, YEAR, QUARTER)
    WITH date_range AS (
      SELECT
        DATEADD(day, ROW_NUMBER() OVER (ORDER BY seq4()) - 1, '2020-01-01') AS date
      FROM
        TABLE(GENERATOR(rowcount => 2191)) -- 2191 days between '2020-01-01' and '2025-12-31'
    ),
    date_components AS (
      SELECT
        date,
        EXTRACT(day FROM date) AS day,
        EXTRACT(month FROM date) AS month,
        EXTRACT(year FROM date) AS year,
        EXTRACT(quarter FROM date) AS quarter
      FROM
        date_range
    )
    SELECT  date,day,month, year, quarter FROM  date_components
;

SELECT  date,day,month, year, quarter FROM  dm_tempo ;


/* ----------------------------------------------------------------------------- */

// SCRIPT 'intermediate_vendas'

-- STREAM DE 'customers_raw'
CREATE STREAM IF NOT EXISTS stream_clientes ON TABLE prj_db_oltp_vendas.raw.customers_raw;

SELECT * FROM prj_db_oltp_vendas.intermediate_vendas.stream_clientes;
SELECT * FROM prj_db_oltp_vendas.data_mart_vendas.dm_clientes;

CREATE OR REPLACE TASK dm_clientes_tsk
WAREHOUSE = DW_AULA
SCHEDULE = 'USING CRON 1 * * * * America/Fortaleza'
AS
    INSERT INTO prj_db_oltp_vendas.data_mart_vendas.dm_clientes (customer_id, customer_name, email, address)
        SELECT
          sc.customer_id,
          sc.customer_name,
          sc.email,
          sc.address
        FROM prj_db_oltp_vendas.intermediate_vendas.stream_clientes sc
        WHERE sc.metadata$action = 'INSERT'
;


INSERT INTO prj_db_oltp_vendas.raw.customers_raw (customer_id, customer_name, email, address) 
VALUES (4, 'Daimiro-Bru', 'Daimon Salvatore@gmail.com', 'Villagio maraponga')

;
       

SELECT * FROM prj_db_oltp_vendas.raw.customers_raw;


UPDATE prj_db_oltp_vendas.raw.customers_raw
SET    email = 'amoras_amore@gmail.com'
WHERE  customer_id = 3
;

-- UPDATE prj_db_oltp_vendas.raw.customers_raw cr
-- SET 
--     cr.email = sc1.email
-- FROM prj_db_oltp_vendas.intermediate_vendas.stream_clientes sc1
-- WHERE 
--     cr.customer_id = sc1.customer_id AND
--     sc1.metadata$action = 'INSERT'



       
/* ----------------------------------------------------------------------------- */

-- STREAM DE 'products_raw'
CREATE STREAM IF NOT EXISTS stream_produtos ON TABLE prj_db_oltp_vendas.raw.products_raw;

SELECT * FROM prj_db_oltp_vendas.intermediate_vendas.stream_produtos;
SELECT * FROM prj_db_oltp_vendas.data_mart_vendas.dm_produtos;

CREATE OR REPLACE TASK dm_produtos_tsk
WAREHOUSE = DW_AULA
AFTER dm_clientes_tsk
AS
    INSERT INTO prj_db_oltp_vendas.data_mart_vendas.dm_produtos (product_id , product_name, description, price)
        SELECT
          sp.product_id, 
          sp.product_name,
          sp.description, 
          sp.price
        FROM prj_db_oltp_vendas.intermediate_vendas.stream_produtos sp
        WHERE sp.metadata$action = 'INSERT'
;


INSERT INTO prj_db_oltp_vendas.raw.products_raw (product_id, product_name, description, price)
VALUES (4, 'RACAO', 'SHOPEE', 100.00)
;


SELECT * FROM prj_db_oltp_vendas.raw.products_raw;


/* ----------------------------------------------------------------------------- */

-- STREAM DE 'sales_raw'
CREATE STREAM IF NOT EXISTS stream_vendas ON TABLE prj_db_oltp_vendas.raw.sales_raw;

SELECT * FROM prj_db_oltp_vendas.intermediate_vendas.stream_vendas;
SELECT * FROM prj_db_oltp_vendas.data_mart_vendas.fc_vendas;

DROP TASK dm_vendas_tsk;

CREATE OR REPLACE TASK fc_vendas_tsk
WAREHOUSE = DW_AULA
AFTER dm_produtos_tsk
AS
    INSERT INTO prj_db_oltp_vendas.data_mart_vendas.fc_vendas (sale_id, customer_sk , product_sk , date_sk, quantity)
    SELECT
      ss.sale_id,
      (
        SELECT
            MAX(customer_sk) 
        FROM prj_db_oltp_vendas.data_mart_vendas.dm_clientes 
        WHERE customer_id = ss.customer_id
        ) AS customer_sk,
      (
        SELECT 
            MAX(product_sk) 
        FROM prj_db_oltp_vendas.data_mart_vendas.dm_produtos 
        WHERE product_id = ss.product_id
      ) AS product_sk,
      (
        SELECT 
            MAX(date_sk) 
        FROM prj_db_oltp_vendas.data_mart_vendas.dm_tempo 
        WHERE date = ss.sale_date
      ) AS date_sk,
      ss.quantity
    FROM
      prj_db_oltp_vendas.intermediate_vendas.stream_vendas ss
    WHERE
      ss.metadata$action = 'INSERT'
;


INSERT INTO prj_db_oltp_vendas.raw.sales_raw (sale_id, customer_id, product_id, sale_date, quantity)
VALUES (4, 4, 4, '2022-01-03', 13)
;


SELECT * FROM prj_db_oltp_vendas.raw.sales_raw;
       

/* ----------------------------------------------------------------------------- */

// ATICANDO AS TASKs

ALTER TASK fc_vendas_tsk RESUME;
ALTER TASK dm_produtos_tsk RESUME;
ALTER TASK dm_clientes_tsk RESUME;

/* ----------------------------------------------------------------------------- */