
// ORIGEM: SHIPPERS
// STREAM: SHIPPERS_STREAM
// DESTINATION: SHIPPERS_RAW

-- TESTAR COMPORTAMENTO COM OPERAÇÕES DML
-- INSERT, DELETE e UPDATE


-- ORIGINAL
SELECT * FROM shippers
;


-- DESTINO 
CREATE OR REPLACE TABLE shippers_raw (
    shipper_id SMALLINT NOT NULL,
    company_name STRING NOT NULL,
    phone STRING
)
;


-- STREAM
CREATE STREAM shippers_stream ON TABLE shippers
;


SELECT
    *
FROM shippers_stream
;


/* ***************************************************** */
// INSERT

INSERT INTO shippers VALUES (
    5, 'EMPRESA XPTO LTDA', '(85) 98888-7777'
)
;

-- VALIDANDO A APTURA PARA 'shippers_stream'
SELECT * FROM shippers;
SELECT * FROM shippers_stream;


-- INSERT DE REGISTRO
INSERT INTO shippers_raw
    SELECT
        shipper_id,
        company_name,
        phone
    FROM shippers_stream
    WHERE 
        metadata$action = 'INSERT' AND
        metadata$isupdate = 'FALSE'

;

-- TESTANDO A INSERÇÃO PARA 'shippers_raw'
-- E EXCLUSÃO EM AUTOMÁTICA EM 'shippers_stream'
SELECT * FROM shippers_raw;
SELECT * FROM shippers_stream;


/* ***************************************************** */
// UPDATE


-- ATUALIZANDO A TABELA 'shippers'
UPDATE shippers
SET company_name = 'XPTO LIMITADA'
WHERE shipper_id = 5
;


-- TESTANDO A RECUPERAÇÃO e CRIAÇÃO
-- DA 'SCD 2' EM 'shippers_stream'
SELECT * FROM shippers;
SELECT * FROM shippers_stream;
SELECT * FROM shippers_raw;


-- adc
UPDATE shippers_raw sr
SET
    sr.company_name = ss.company_name,
    sr.phone = ss.phone
FROM shippers_stream ss
WHERE
    ss.shipper_id = sr.shipper_id AND 
    ss.metadata$action = 'INSERT' AND
    ss.metadata$isupdate = 'TRUE'
;



/* ***************************************************** */
// DELETE


-- DELETAR A LINHA INSERIDA EM 'shippers'
DELETE FROM shippers 
WHERE shipper_id = 5
;


SELECT * FROM shippers;
SELECT * FROM shippers_stream;
SELECT * FROM shippers_raw;


DELETE FROM shippers_raw sr
WHERE sr.shipper_id IN( 
    SELECT
        ss.shipper_id
    FROM shippers_stream ss
    WHERE 
        ss.metadata$action = 'DELETE' AND
        ss.metadata$isupdate = 'FALSE'
)
;