import psycopg2
import pandas as pd

# Conectando ao banco de dados
stringConexao = "dbname='de' host='localhost' user='postgres' password='post'"

conexao = psycopg2.connect(stringConexao)

# Realizar a consulta e obter os dados
dados = pd.read_sql_query(
    "SELECT COUNT(*) AS total_dados FROM dados_covid WHERE comorbidades is not null and UF = 'AL'",
    conexao)
total = pd.read_sql_query("SELECT COUNT(*) AS total FROM dados_covid where UF = 'AL'", conexao)

dados_sc = pd.read_sql_query(
    "SELECT COUNT(*) AS total_dados_sc FROM dados_covid WHERE comorbidades is not null and UF = 'SC'",
    conexao)
total_sc = pd.read_sql_query("SELECT COUNT(*) AS total_sc FROM dados_covid where UF ='SC'", conexao)

# Obter o valor total e total de dados recuperados
total_val = total['total'].values[0]
dados_recuperados = dados['total_dados'].values[0]

total_val_sc = total_sc['total_sc'].values[0]
dados_recuperados_sc = dados_sc['total_dados_sc'].values[0]

# Calcular a porcentagem
porcentagem = (dados_recuperados / total_val) * 100
porcentagem_sc = (dados_recuperados_sc / total_val_sc) * 100

query = """
   SELECT comorbidades, COUNT(*) as quantidade FROM dados_covid WHERE comorbidades IS NOT NULL AND UF = 'AL' GROUP BY comorbidades ORDER BY quantidade DESC;
"""

query_sc = """
    SELECT comorbidades, COUNT(*) as quantidade FROM dados_covid WHERE comorbidades IS NOT NULL AND UF = 'SC' GROUP BY comorbidades ORDER BY quantidade DESC;
"""

df = pd.read_sql_query(query, conexao)
df_sc = pd.read_sql_query(query_sc, conexao)

# Exibindo a lista de comorbidades

# Imprimir os resultados

# Exibir o resultado
print(f"A porcentagem de casos com comorbidades em Alagoas é de: {porcentagem:.2f}%")
print(f"A porcentagem de casos com comorbidades em Santa Catarina é de: {porcentagem_sc:.2f}%")
print("Lista de comorbidades coletadas em Alagoas:")
print("Quantidade\tDescrição")
for index, row in df.iterrows():
    quantidade = row['quantidade']
    descricao = row['comorbidades']
    print(f"{quantidade}\t\t{descricao}")
print("Lista de comorbidades coletadas em Santa Catarina:")
for index, row in df_sc.iterrows():
    quantidade = row['quantidade']
    descricao = row['comorbidades']
    print(f"{quantidade}\t\t{descricao}")

conexao.close()
