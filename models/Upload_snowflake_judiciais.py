from config_snowflake import SnowflakeConnector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import re
import numpy as np
from functools import reduce
from operator import concat

arquivo=r'C:\Users\gpira\Downloads\bq-results-20240515-184938-1715799987947.json'

df=pd.read_json(arquivo,lines=True)

df.assuntosCNJ.isna().unique()

#criação de função para identifcar o tamanho da lista dentro do campo aninhado assuntosCNJ

def itens_lista(df,nome_coluna,nome_coluna_final):
    
    """ conta quantos itens tem na coluna aninhada, isso serve para entendermos quais campos temos dentro dos dicionarios
    Args:
    df: O DataFrame Pandas.
    column_name: O nome da coluna que contém as listas.

  Returns:
    Uma nova coluna no DataFrame com o nome "nome_coluna_final" contendo o número de itens em cada lista.
  """
    
    df[nome_coluna_final] = df[nome_coluna].apply(len)
    return df
    
df=itens_lista(df,'assuntosCNJ','item_count')

df_filtrado=df.loc[df['item_count']>0]
df_filtrado['assuntosCNJ']


indexes_df=df_filtrado.index

lista_parametros=[]
for i in range(len(indexes_df)):
    
    lista_parametros.append(list(df_filtrado['assuntosCNJ'][indexes_df[i]][0].keys()))


lista_unica = reduce(concat, lista_parametros)

#identificamos quais os campos unicos dos dicionarios

print(set(lista_unica)) 


df=itens_lista(df,'partes','cont_partes')

"""Vamos fazer um filtro e selecionar apenas os que contem 1 ou mais itens"""

df_filtrado2=df.loc[df['cont_partes']>0]
df_filtrado2['partes']

#pegamos os indices para passarmos linha a linha do df novo
indexes=df_filtrado2.index
#passamos linha a linha e salvamos tudo em uma lista para conferir posteriormente
lista_parametros_partes=[]
for i in range(len(indexes)):
    
    
    lista_parametros_partes.append(list(df_filtrado2['partes'][indexes[i]][0].keys()))


#conferimos valores unicos

lista_unica = reduce(concat, lista_parametros_partes)

print(set(lista_unica))

#funções para extrair da coluna partes
def extrai_partes_nome1(row):
    try:
        return row.partes[0]['nomeNormalizadoNeoway']
    except (IndexError, KeyError):
        return ' '
def extrai_partes_polo1(row):
    try:
        return row.partes[0]['polo']
    except (IndexError, KeyError):
        return ' '
def extrai_partes_nome2(row):
    try:
        return row.partes[1]['nomeNormalizadoNeoway']
    except (IndexError, KeyError):
        return ' '
def extrai_partes_polo2(row):
    try:
        return row.partes[1]['polo']
    except (IndexError, KeyError):
        return ' '
    
#funções para extrair da coluna assuntoscnj

def extrair_titulo(row):
    try:
        return row.assuntosCNJ[0]['titulo']
    except (IndexError, KeyError):
        return ' '
def extrair_codigolocal(row):
    try:
        return row.assuntosCNJ[0]['codigoLocal']
    except (IndexError, KeyError):
        return ' '
def extrair_codigocnj(row):
    try:
        return row.assuntosCNJ[0]['codigoCNJ']
    except (IndexError, KeyError):
        return ' '
    
#adicionando os casos as colunas especificas

df.loc[:, 'tituloCNJ'] = df.apply(extrair_titulo, axis=1)
df.loc[:, 'codigolocalCNJ'] = df.apply(extrair_codigolocal, axis=1)
df.loc[:, 'codigoCNJ'] = df.apply(extrair_codigocnj, axis=1)
df.loc[:, 'nomeNormalizadoNeowayAtivo'] = df.apply(extrai_partes_nome1, axis=1)
df.loc[:, 'PoloAtivo'] = df.apply(extrai_partes_polo1, axis=1)
df.loc[:, 'nomeNormalizadoNeowayPassivo'] = df.apply(extrai_partes_nome2, axis=1)
df.loc[:, 'PoloPassivo'] = df.apply(extrai_partes_polo2, axis=1)


df=df.drop(columns=['partes','assuntosCNJ','item_count','cont_partes'])
df.replace(' ', np.nan, inplace=True)
print(df.head())

snowflake_conn = SnowflakeConnector()
conn = snowflake_conn.connect()


conn.cursor().execute("USE EMPRESAS.DB_EMPRESAS")
conn.cursor().execute("""CREATE OR REPLACE TABLE EMPRESAS.DB_EMPRESAS.EMPRESAS_PROCESSOS_JUDICIAIS (
                            CNPJ NUMBER(38,0),
                            AREA VARCHAR(16777216),
                            GRAUPROCESSO NUMBER(38,0),
                            COMARCA VARCHAR(16777216),
                            JULGAMENTO VARCHAR(16777216),
                            DATADECISAO TIMESTAMP_NTZ(9),
                            DATAENCERRAMENTO TIMESTAMP_NTZ(9),
                            UF VARCHAR(16777216),
                            TRIBUNAL VARCHAR(16777216),
                            ULTIMOESTADO VARCHAR(16777216),
                            ORGAOJULGADOR VARCHAR(16777216),
                            CITACAOTIPO VARCHAR(16777216),
                            UNIDADEORIGEM VARCHAR(16777216),
                            JUIZ VARCHAR(16777216),
                            VALORCAUSA NUMBER(38,2),
                            VALORPREDICAOCONDENACAO NUMBER(38,13),
                            TITULOCNJ VARCHAR(16777216),
                            CODIGOLOCALCNJ NUMBER(38,0),
                            CODIGOCNJ VARCHAR(16777216),
                            NOMENORMALIZADONEOWAYATIVO VARCHAR(16777216),
                            POLOATIVO VARCHAR(16777216),
                            NOMENORMALIZADONEOWAYPASSIVO VARCHAR(16777216),
                            POLOPASSIVO VARCHAR(16777216)
                            )""")


df = df.rename(columns=lambda x: re.sub('\.', '_', x).upper())


write_pandas(
            conn,
            df,
            'EMPRESAS_PROCESSOS_JUDICIAIS',
            'EMPRESAS',
            'DB_EMPRESAS',  
             )

snowflake_conn.close_connection()
