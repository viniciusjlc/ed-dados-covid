import psycopg2
import pandas as pd
import numpy as np

stringConexao = "dbname='de' host='localhost' user='postgres' password='post'"

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
      "where \"Data de atendimento no Serviço\" >= '2020-01-01' " \
      "and \"Data de atendimento no Serviço\" <= '2023-01-01' " \
      "from covid_maceio " \
      "order by data_atendimento"

dataFrame = pd.read_sql(sql, conexao)

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

# tratando situacao
dataFrame['situacao'] = dataFrame['situacao'].str.strip()
dataFrame['situacao'] = dataFrame['situacao'].str.upper()

# tratando municipios
dataFrame['municipio'] = dataFrame['municipio'].str.strip()
dataFrame['municipio'] = dataFrame['municipio'].str.upper()


colunas = ",".join([str(i) for i in dataFrame.columns.tolist()])
print("INICIO INSERT")
for i, row in dataFrame.iterrows():
    sql = "insert into dados_covid (" + colunas + ") values (" + "%s," * (len(row) - 1) + "%s)"
    cursor.execute(sql, tuple(row))

conexao.commit()
print("FIM INSERT")
