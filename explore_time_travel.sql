SELECT
    *
FROM shippers
;

INSERT INTO shippers VALUES(4,'snowfaker','(85) 98421-8726');

\\ RECUPERANDO DADOS DE 90SEGUNDOS ANTES DA INCLUSAO 
SELECT
    *
FROM shippers
AT (OFFSET => -90);


SELECT current_timestamp();

SELECT
    *
FROM shippers
BEFORE (TIMESTAMP =>'2025-09-20 16:30:07.568 -0700'::timestamp);