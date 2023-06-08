import psycopg2
import pandas as pd

stringConexao = "dbname='de' host='localhost' user='postgres' password='post'"

conexao = psycopg2.connect(stringConexao)

query = "SELECT AVG(idade) FROM dados_covid where UF = 'AL'"
query_sc = "SELECT AVG(idade) FROM dados_covid where UF = 'SC'"
df = pd.read_sql_query(query, conexao)
df_sc = pd.read_sql_query(query_sc, conexao)

# Obtendo a média das idades a partir do DataFrame
media_idade = df.iloc[0, 0]
media_idade_sc = df_sc.iloc[0, 0]

# Exibindo o resultado com apenas dois dígitos após a vírgula
media_formatada = round(media_idade, 2)
media_formatada_sc = round(media_idade_sc, 2)
print("A média de idades nos casos de COVID em Alagoas é:", media_formatada, ", já em Santa Catarina é: ", media_formatada_sc)

# Fechando a conexão com o banco de dados
conexao.close()