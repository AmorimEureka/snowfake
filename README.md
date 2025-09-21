SNOWFLAKE
=============================================================

<br>
 O projeto para explorar snowflake.

<br>
<br>
<br>

![WSL](https://img.shields.io/badge/WSL-2.0+-brightgreen?logo=windows&logoColor=white)
[![DBeaver](https://img.shields.io/badge/DBeaver-Tool-372923?logo=dbeaver&logoColor=white)](https://dbeaver.io/)
[![Snowflake Docs](https://img.shields.io/badge/Snowflake-Docs-29B5E8?logo=snowflake&logoColor=white)](https://docs.snowflake.com/)
[![ANSI SQL](https://img.shields.io/badge/ANSI_SQL-Standard-2F74C0?logo=sqlite&logoColor=white)](#)

<br>
<br>



<details close>
  <summary>
    <strong>SNOWFLAKE❄:</strong>
  </summary>

<br>

- **Criado por Experts em Data Warehouse da Oracle**

<br>

- **Stack do tipo SaaS: Soft as a Service [Software como Serviço]**


    - Assinatura
    - Acessível pela web browser
    - Atualizações automáticas
    - Escalabilidade
    - Abstração da Infra

<br>

- **Roda sobre plataformas Cloud**
    - AWS
    - GCP
    - Azure
</details>

<br>

<details close>
  <summary>
    <strong>Contratos:</strong>
  </summary>

<br>

- **Virtual Private SnowFlake**
    - Ambiente isolado e privado
        - Empresas de regulamentação especifica

</details>

<br>

<details close>
  <summary>
    <strong>Organização:</strong>
  </summary>

<br>

- **WAREHOUSES:** Provisionamento de recursos computacionais para executar processamento possiilitando agrupar custos são calculados por processamento

<br>

- **TAMANHO | CREDITO/HORA**: 

<br>

- **QUERY ACCELERATION:**
    - Uso de GPU
    - Scale Factor
    - Consultas Complexas
    - Custos adicionais

<br>

- **MULT-CLUSTER**
    - Mode:
        - Auto-Scale:
            - Ajuste automático por demanda
            - Definição do número mínimo e máximo de Clusters
            - Scalling Policy:
                - Standard:
                    - Add/Remove recursos por demanda
                - Economy:
                    - Mais conservador
        - Maximized
            - Aloca o máximo de recurso
        
        - Auto-Suspend:
            - Encerra o uso recursos após o período de inatividade
            
        - Suspend After:
            - Defini o tempo para encerramento em min
            
        - Auto-Resume:
            - Retoma os recursos quando há demanda

</details>

<br>

<details close>
  <summary>
    <strong>VIEWs:</strong>
  </summary>

<br>

- **MATERIALIZADAS**
    - Consulta uma única tabela
    - Não suporta:
        - UDF
        - Windows Functions
        - Having
        - Order by
        - Limit
        - Group by
</details>

<br>

<details close>
  <summary>
    <strong>TIME TRAVEL:</strong>
  </summary>

<br>

- **TIME TRAVEL**
    - Acesso a dados historicos
        - Dados excluídos
        - Consultar estados anteriores
        - Clonar esses dados
        - entre 1 e 90 dias
</details>


<br>

<details close>
  <summary>
    <strong>FAIL-SAFE:</strong>
  </summary>

<br>

- **FAIL-SAFE**
    - Mitigação de desastres
    - Intervalo de 7dias no qual os dados podem ser recuperados
    - Inicio a partir do findo do TIME-TRAVEL
    - Falar com suporte
</details>


<br>

<details close>
  <summary>
    <strong>TASKS:</strong>
  </summary>

<br>

- Mitigação de desastres
- Intervalo de 7dias no qual os dados podem ser recuperados
- Inicio a partir do findo do TIME-TRAVEL
- Falar com suporte
</details>


<br>

<details close>
  <summary>
    <strong>Agendamento:</strong>
  </summary>

<br>

- Schedule Crontab:
    
    
    | * | * | * | * | * |
    | --- | --- | --- | --- | --- |
    | minutos | horas | dias Month | mês | dia Week |
    | (0-59) | (0-23) | (1-31); L | (1-12); (JAN-DEC) | (0-6); (SUN-SAT); L |

- Child Task:
    - Hierarquia de tasks
    - Acíclica

</details>


<br>


<details close>
  <summary>
    <strong>STREAMS & CDC:</strong>
  </summary>

<br>

- Stack para captura alterações do OLTP para OLAP

- Propósito:
    - Integração de dados
    - Recuperação de desastres
    
- Funcionamento:
    - ELEMENTOS:
        - Tabela monitorada
        - Tabela de controle CDC
            - Colunas de controle
                
                
                | ATTRIBUTES TRACK | VALUES | INSERT | UPDATE | DELETE |
                | --- | --- | --- | --- | --- |
                | METADATA$ACTION | INSERT/DELETE | INSERT | new: INSERT
                old :  DELETE | DELETE |
                | METADATA$ISUPDATE | TRUE/FALSE | FALSE | new: TRUE
                old  : TRUE | FALSE |
            - Dados são apagados ao atualizar a tabela final
        - Tabela final
    - Atributos replicados na tabela intermediaria
    - Monitoramento de operações DML [INSERT, DELETE e UPDATE]

- Log de alterações

- Append-Only
    - Captura Insert
</details>