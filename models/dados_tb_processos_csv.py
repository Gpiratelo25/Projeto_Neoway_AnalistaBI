from config_snowflake import SnowflakeConnector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import re

snowflake_conn = SnowflakeConnector()
conn = snowflake_conn.connect()
cursor = conn.cursor()
cursor.execute("SELECT * FROM  EMPRESAS.DB_EMPRESAS.DF_PROCESSOS_JUDICIAIS")
columns = [col[0] for col in cursor.description]

# Buscar todos os dados da consulta
data = cursor.fetchall()

# Criar um DataFrame do Pandas usando os dados e os nomes das colunas
df = pd.DataFrame(data, columns=columns)
print(df.head())

df.to_csv(r'C:\DBT\project_neoway\dados_tb_processos_judiciais.csv',index=False)