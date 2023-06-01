import psycopg2
import pandas as pd
import numpy as np

stringConexao = "dbname='de' host='localhost' user='postgres' password='postgres'"

conexao = psycopg2.connect(stringConexao)

cursor = conexao.cursor()

sql = "select " \
      "\"Data de atendimento no Serviço\"                                as data_atendimento, " \
      "\"IDADE\"                                                         as idade, " \
      "\"SEXO\"                                                          as sexo, " \
      "\"MUNICÍPIO\"                                                     as municipio, " \
      "'ALAGOAS'                                                         as estado, " \
      "'AL'                                                              as uf, " \
      "\"CLASSIFICAÇÃO (Confirmado, suspeito, descartado, óbito, cura\"  as classificacao, " \
      "\"Comorbidades\"                                                  as comorbidades, " \
      "\"Situação do paciente confirmado (UTI, isolamento domiciliar, \" as situacao, " \
      "\"Data do Óbito (Caso haja)\"                                     as data_obito " \
      "from covid_maceio " \
      "order by data_atendimento"

dataFrame = pd.read_sql(sql, conexao)

print(dataFrame.columns)

print(dataFrame[['data_atendimento', 'idade', 'sexo', 'comorbidades', 'data_obito']].head(10))

# tratando data_atendimento
dataFrame['data_atendimento'] = pd.to_datetime(dataFrame['data_atendimento'], format="ISO8601")

# tratando idade
dataFrame = dataFrame[dataFrame.idade.apply(lambda valor: valor.isnumeric())]

# tratando sexo
dataFrame['sexo'] = dataFrame['sexo'].str.strip()
dataFrame['sexo'] = dataFrame['sexo'].str.upper()

# tratando comorbidades
dataFrame['comorbidades'] = dataFrame['comorbidades'].str.strip()
dataFrame['comorbidades'] = dataFrame['comorbidades'].str.upper()
dataFrame['comorbidades'] = dataFrame['comorbidades'].replace('/', ',', regex=True)
dataFrame['comorbidades'] = dataFrame['comorbidades'].replace('SEM COMORBIDADE', None, regex=True)
dataFrame['comorbidades'] = dataFrame['comorbidades'].replace('SEM INFORMAÇÃO', None, regex=True)
dataFrame['comorbidades'] = dataFrame['comorbidades'].replace('RECUPERADO', None, regex=True)

# tratando data_obito
dataFrame['data_obito'] = pd.to_datetime(dataFrame['data_obito'], format="ISO8601")
dataFrame['data_obito'] = dataFrame['data_obito'].replace({np.nan: None})

# tratando situacao
dataFrame['situacao'] = dataFrame['situacao'].str.strip()
dataFrame['situacao'] = dataFrame['situacao'].str.upper()

print(dataFrame[['data_atendimento', 'idade', 'sexo', 'comorbidades', 'data_obito']].head(10))

colunas = ",".join([str(i) for i in dataFrame.columns.tolist()])

for i, row in dataFrame.iterrows():
    sql = "insert into dados_covid (" + colunas + ") values (" + "%s," * (len(row) - 1) + "%s)"
    cursor.execute(sql, tuple(row))

conexao.commit()

print("FINALIZADO")
