import psycopg2
import pandas as pd
import numpy as np

stringConexao = "dbname='de' host='localhost' user='postgres' password='post'"

conexao = psycopg2.connect(stringConexao)

cursor = conexao.cursor()

sql = "select " \
      "data_coleta                       as data_atendimento, " \
      "idade, " \
      "sexo, " \
      "municipio, " \
      "'SANTA CATARINA'                  as estado, " \
      "'SC'                              as uf, " \
      "classificacao, " \
      "comorbidades, " \
      "recuperados                       as situacao, " \
      "to_date(data_obito, 'DD/MM/YYYY') as data_obito " \
      "where data_coleta >= '01/01/2020' " \
      "and data_coleta <= '01/01/2023' " \
      "from covid_boavista " \
      "order by data_atendimento"

dataFrame = pd.read_sql(sql, conexao)

print(dataFrame.columns)

print(dataFrame[['data_atendimento', 'idade', 'sexo', 'comorbidades', 'data_obito']].head(10))

# tratando data_atendimento
dataFrame['data_atendimento'] = dataFrame['data_atendimento'].replace(r'[a-zA-Z]', np.nan, regex=True)
dataFrame = dataFrame.dropna(subset=['data_atendimento'])
dataFrame['data_atendimento'] = pd.to_datetime(dataFrame['data_atendimento'], format="%d/%m/%Y")

# tratando idade
dataFrame = dataFrame.dropna(subset=['idade'])
dataFrame['idade'] = dataFrame['idade'].apply(np.int64)

# tratando data_obito
dataFrame['data_obito'] = dataFrame['data_obito'].replace({np.nan: None})

# tratando sexo
dataFrame['sexo'] = dataFrame['sexo'].replace('FEMININO', 'F', regex=True)
dataFrame['sexo'] = dataFrame['sexo'].replace('MASCULINO', 'M', regex=True)
dataFrame['sexo'] = dataFrame['sexo'].replace('NAO INFORMADO', np.nan, regex=True)
dataFrame = dataFrame.dropna(subset=['sexo'])

# tratando situacao
dataFrame['situacao'] = dataFrame['situacao'].replace('SIM', 'RECUPERADO', regex=True)
dataFrame['situacao'] = dataFrame['situacao'].replace('NÃO', 'ÓBITO', regex=True)
dataFrame['situacao'] = dataFrame['situacao'].replace('NAO', 'ÓBITO', regex=True)

print(dataFrame[['data_atendimento', 'idade', 'sexo', 'comorbidades', 'data_obito']].head(10))

colunas = ",".join([str(i) for i in dataFrame.columns.tolist()])
print("INICIO INSERT")
for i, row in dataFrame.iterrows():
      sql = "insert into dados_covid (" + colunas + ") values (" + "%s," * (len(row) - 1) + "%s)"
      cursor.execute(sql, tuple(row))

conexao.commit()
print("FIM INSERT")
