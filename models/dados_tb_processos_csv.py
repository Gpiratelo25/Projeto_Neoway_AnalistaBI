"""
CRIEI ESSA CONSULTA PARA PEGAR OS DADOS DA VIEW CRIADOS ANTERIORMENTE,
VISANDO UTILIZAR UM ARQUIVO CSV NO POWER BI PARA CASO OS RECRUTADORES POSSAM ABRIR O ARQUIVO PBI EM OUTRAS MÁQUINAS"""


from config_snowflake import SnowflakeConnector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import re

snowflake_conn = SnowflakeConnector()
conn = snowflake_conn.connect()
cursor = conn.cursor()
cursor.execute("SELECT * FROM  EMPRESAS.DB_EMPRESAS.DF_PROCESSOS_JUDICIAIS")
columns = [col[0] for col in cursor.description]


data = cursor.fetchall()


df = pd.DataFrame(data, columns=columns)


df.to_csv(r'C:\DBT\project_neoway\dados_tb_processos_judiciais.csv',index=False)
