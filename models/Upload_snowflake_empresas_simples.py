from config_snowflake import SnowflakeConnector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import re

snowflake_conn = SnowflakeConnector()
conn = snowflake_conn.connect()

conn.cursor().execute("USE EMPRESAS.DB_EMPRESAS")
conn.cursor().execute("""CREATE OR REPLACE TABLE EMPRESAS.DB_EMPRESAS.EMPRESAS_SIMPLES (
                            CNPJ NUMBER(38,0),
                            OPTANTE_SIMPLES VARCHAR(16777216),
                            OPTANTE_SIMEI VARCHAR(16777216)
                            )""")

df1=pd.read_csv(r'C:\Users\gpira\Downloads\empresas_simples.csv',sep=';')
df1 = df1.rename(columns=lambda x: re.sub('\.', '_', x).upper())


write_pandas(
            conn,
            df1,
            'EMPRESAS_SIMPLES',
            'EMPRESAS',
            'DB_EMPRESAS',  
             )

snowflake_conn.close_connection()