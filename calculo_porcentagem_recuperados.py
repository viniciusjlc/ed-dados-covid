import psycopg2
import pandas as pd

# Conectando ao banco de dados
stringConexao = "dbname='de' host='localhost' user='postgres' password='post'"

conexao = psycopg2.connect(stringConexao)

# Realizar a consulta e obter os dados
dados = pd.read_sql_query("SELECT COUNT(*) AS total_dados FROM dados_covid WHERE situacao = 'RECUPERADO' and UF = 'AL'",
                          conexao)
total = pd.read_sql_query("SELECT COUNT(*) AS total FROM dados_covid where UF = 'AL'", conexao)

dados_sc = pd.read_sql_query("SELECT COUNT(*) AS total_dados_sc FROM dados_covid WHERE situacao = 'RECUPERADO' and UF = 'SC'",
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

# Consulta SQL AL
consulta_al = '''
SELECT total.municipio, total.count AS total_casos, total_recuperado.count AS total_recuperado
FROM (
    SELECT COUNT(*) AS count, municipio
    FROM dados_covid
    WHERE uf = 'AL'
    GROUP BY municipio
) AS total
JOIN (
    SELECT COUNT(*) AS count, municipio
    FROM dados_covid
    WHERE uf = 'AL' and situacao = 'RECUPERADO'
    GROUP BY municipio
) AS total_recuperado
ON total.municipio = total_recuperado.municipio;
'''


# Consulta SQL SC
consulta_sc = '''
SELECT total.municipio, total.count AS total_casos, total_recuperado.count AS total_recuperado
FROM (
    SELECT COUNT(*) AS count, municipio
    FROM dados_covid
    WHERE uf = 'SC'
    GROUP BY municipio
) AS total
JOIN (
    SELECT COUNT(*) AS count, municipio
    FROM dados_covid
    WHERE uf = 'SC' AND situacao = 'RECUPERADO'
    GROUP BY municipio
) AS total_recuperado
ON total.municipio = total_recuperado.municipio;
'''

# Ler os dados da consulta SQL
df_recuperado_al = pd.read_sql_query(consulta_al, conexao)
df_recuperado_sc = pd.read_sql_query(consulta_sc, conexao)

# Imprimir os resultados

# Exibir o resultado
print(f"A porcentagem de casos recuperados em Alagoas é de: {porcentagem:.2f}%")
print(df_recuperado_al)
print(f"A porcentagem de casos recuperados em Santa Catarina é de: {porcentagem_sc:.2f}%")
print(df_recuperado_sc)


conexao.close()
