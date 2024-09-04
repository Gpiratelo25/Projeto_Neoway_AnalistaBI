from config_snowflake import SnowflakeConnector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import re

snowflake_conn = SnowflakeConnector()
conn = snowflake_conn.connect()

conn.cursor().execute("CREATE OR REPLACE DATABASE EMPRESAS")
conn.cursor().execute("CREATE OR REPLACE SCHEMA DB_EMPRESAS")
conn.cursor().execute("""CREATE OR REPLACE TABLE EMPRESAS.DB_EMPRESAS.DF_EMPRESAS (
                        CNPJ NUMBER(38,0),
                        DT_ABERTURA DATE,
                        MATRIZ_EMPRESAMATRIZ BOOLEAN,
                        CD_CNAE_PRINCIPAL VARCHAR(16777216),
                        DE_CNAE_PRINCIPAL VARCHAR(16777216),
                        DE_RAMO_ATIVIDADE VARCHAR(16777216),
                        DE_SETOR VARCHAR(16777216),
                        ENDERECO_CEP NUMBER(38,0),
                        ENDERECO_MUNICIPIO VARCHAR(16777216),
                        ENDERECO_UF VARCHAR(16777216),
                        ENDERECO_REGIAO VARCHAR(16777216),
                        ENDERECO_MESORREGIAO VARCHAR(16777216),
                        SITUACAO_CADASTRAL VARCHAR(16777216))""")

df1=pd.read_csv(r'C:\Users\gpira\Downloads\df_empresas.csv',sep=';')
# df1 = df1.rename(columns=lambda x: x.upper())
df1 = df1.rename(columns=lambda x: re.sub('\.', '_', x).upper())


write_pandas(
            conn,
            df1,
            'DF_EMPRESAS',
            'EMPRESAS',
            'DB_EMPRESAS',  
             )

snowflake_conn.close_connection()