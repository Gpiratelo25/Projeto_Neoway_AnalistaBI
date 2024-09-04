<h1 align="center">Desafio Neoway</h1>

A Neoway está com uma vaga para Analista de BI para atuar em Home Office. A
equipe de BI da Neoway trabalha na construção de aplicações de Business Intelligence
para clientes de maior relevância no mercado nacional e internacional. Trabalhamos com a
ferramenta Power BI para a apresentação dos dashboards, porém nosso processo de ETL e
modelagem dimensional é feito no DBT, usando SQL e Python, e consumindo dados
geralmente do Bigquery ou de bancos relacionais.

# Sobre o Teste

O teste consiste na criação de dashboard(s) no Power BI onde a pessoa candidata
deve sugerir análises que julgar pertinente a partir de bases de uma amostra de empresas
do Brasil e uma base de processos jurídicos de algumas dessas empresas. As bases são
disponibilizadas em formato .csv(empresas) e .json(processos) para que a pessoa
candidata possa realizar o seu trabalho de análise e em seguida traçar uma estratégia de
modelagem e apresentação do(s) dashboard(s) no Power BI.
Cabe ao candidato escolher quais indicadores e análises serão apresentados e a
defesa dessas escolhas será uma etapa importante do processo.
Serão analisados os seguintes pontos para avaliar a aptidão do candidato a vaga:
• Qualidade dos scripts desenvolvidos na etapa de ETL;
• Modelagem dos dados;
• Criação dos Dashboards com o uso do Power BI;
• Uso das técnicas de Storytelling na criação dos dashboards;
• Análises propostas a partir das bases de dados fornecidas;
• Argumentação e apresentação da solução proposta;


# Sumário

- [Construção do desafio](#construção-do-desafio)
- [Desenvolvimento](#Desenvolvimento)




# Construção do desafio

O desenvolvimento ETL do projeto foi feito com: dbt,python,snowflake

# Desenvolvimento
* Estabeleço a conexão entre dbt, VScode e snowflake: [models/config_snowflake.py](https://github.com/Gpiratelo25/Projeto_Neoway_AnalistaBI/blob/Gpiratelo25-patch-1/models/config_snowflake.py)
* Criado o banco de dados no snowflake e feito o primeiro upload de arquivo: [models/Upload_snowflake_df_empresas.py](https://github.com/Gpiratelo25/Projeto_Neoway_AnalistaBI/blob/Gpiratelo25-patch-1/models/Upload_snowflake_df_empresas.py)
* Upload da tabela **EMPRESAS_NIVEL_ATIVIDADE**[models/Upload_snowflake_empresas_nivel_atividade.py](https://github.com/Gpiratelo25/Projeto_Neoway_AnalistaBI/blob/Gpiratelo25-patch-1/models/Upload_snowflake_empresas_nivel_atividade.py)
* Upload da tabela **EMPRESAS_PORTE** [models/Upload_snowflake_empresas_porte.py](https://github.com/Gpiratelo25/Projeto_Neoway_AnalistaBI/blob/Gpiratelo25-patch-1/models/Upload_snowflake_empresas_porte.py)
* Upload da tabela **EMPRESAS_SAUDE_TRIBUTARIA**[models/Upload_snowflake_empresas_saude_tributaria.py](https://github.com/Gpiratelo25/Projeto_Neoway_AnalistaBI/blob/Gpiratelo25-patch-1/models/Upload_snowflake_empresas_saude_tributaria.py)
* Upload da tabela **EMPRESAS_SIMPLES**[models/Upload_snowflake_empresas_simples.py](https://github.com/Gpiratelo25/Projeto_Neoway_AnalistaBI/blob/Gpiratelo25-patch-1/models/Upload_snowflake_empresas_simples.py)
* Upload do arquivo json **bq-results-20240515-184938-1715799987947**[models/Upload_snowflake_judiciais.py](https://github.com/Gpiratelo25/Projeto_Neoway_AnalistaBI/blob/Gpiratelo25-patch-1/models/Upload_snowflake_judiciais.py)
* Criando a view **PROCESSOS_JUDICIAIS**[models/Criando_view_processos.py](https://github.com/Gpiratelo25/Projeto_Neoway_AnalistaBI/blob/Gpiratelo25-patch-1/models/Criando_view_processos.py)
* Criação do painel POWER BI [PBI/PAINEL GERENCIAL DE PROCESSOS JUDICIAIS.pbix](https://github.com/Gpiratelo25/Projeto_Neoway_AnalistaBI/blob/Gpiratelo25-patch-1/PBI/PAINEL%20GERENCIAL%20DE%20PROCESSOS%20JUDICIAIS.pbix)
