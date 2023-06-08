import psycopg2
import pandas as pd
import numpy as np

stringConexao = "dbname='de' host='localhost' user='postgres' password='post'"

conexao = psycopg2.connect(stringConexao)

cursor = conexao.cursor()

sql = "select * from dados_covid; "

dataFrame = pd.read_sql(sql, conexao)

total = len(dataFrame.index)

totalF = dataFrame['sexo'].value_counts()['F']
totalM = dataFrame['sexo'].value_counts()['M']
print("Total: ", total)
print("TotalF: ", totalF)
print("TotalM: ", totalF)
print("%F: ", round(((totalF / total) * 100), 2), "%")
print("%M: ", round(((totalM / total) * 100), 2), "%")

sql_criar_tabela_estatisticas_sexo = "create table estatisticas_sexo (" \
                    "id serial not null primary key," \
                    "campo varchar not null," \
                    "valor bigint not null," \
                    "porcentagem float8 not null" \
    ");"

cursor.execute(sql_criar_tabela_estatisticas_sexo)

d = {'campo': ['total', 'feminino', 'masculino'],
     'valor': [total, totalF, totalM],
     'porcentagem': [100, round(((totalF / total) * 100), 2), round(((totalM / total) * 100), 2)]}
df = pd.DataFrame(data=d)

colunas = ",".join([str(i) for i in df.columns.tolist()])
print("INICIO INSERT")
print(colunas)
for i, row in df.iterrows():
    sql = "insert into estatisticas_sexo (" + colunas + ") values (" + "%s," * (len(row) - 1) + "%s)"
    cursor.execute(sql, tuple(row))
print("FIM INSERT")





totalRecuperados = dataFrame['situacao'].value_counts()['RECUPERADO'] + dataFrame['situacao'].value_counts()[
'HOSPITALIZADO'] + dataFrame['situacao'].value_counts()['ISOLAMENTO DOMICILIAR']
totalObtidosCovid = dataFrame['situacao'].value_counts()['ÓBITO']
totalObtidosOutros = dataFrame['situacao'].value_counts()['ÓBITO POR OUTRAS CAUSAS']

print("Total: ", total)
print("totalRecuperados: ", totalRecuperados)
print("totalObtidosCovid: ", totalObtidosCovid)
print("totalObtidosOutros: ", totalObtidosOutros)
print("%Recuperados: ", round(((totalRecuperados / total) * 100), 2), "%")
print("%ObtidosCovid: ", round(((totalObtidosCovid / total) * 100), 2), "%")
print("%ObtidosOutros: ", round(((totalObtidosOutros / total) * 100), 2), "%")

totalSemComorbidade = dataFrame['comorbidades'].isna().sum()
totalComComorbidade = dataFrame['comorbidades'].notna().sum()
print("Total: ", total)
print("totalSemComorbidade: ", totalSemComorbidade)
print("totalComComorbidade: ", totalComComorbidade)
print("%SemComorbidade: ", round(((totalSemComorbidade / total) * 100), 2), "%")
print("%ComComorbidade: ", round(((totalComComorbidade / total) * 100), 2), "%")

conexao.commit()
