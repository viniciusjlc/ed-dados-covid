import psycopg2
import pandas as pd
import numpy as np

# Conectando ao banco de dados
stringConexao = "dbname='de' host='localhost' user='postgres' password='post'"

conexao = psycopg2.connect(stringConexao)


# Consulta SQL
consulta = '''
SELECT EXTRACT(YEAR FROM data_atendimento) AS ano,
       SUM(CASE WHEN uf = 'AL' THEN 1 ELSE 0 END) AS casos_AL,
       SUM(CASE WHEN uf = 'SC' THEN 1 ELSE 0 END) AS casos_SC,
       ABS(SUM(CASE WHEN uf = 'AL' THEN 1 ELSE 0 END) - SUM(CASE WHEN uf = 'SC' THEN 1 ELSE 0 END)) AS diferenca_casos,
       CASE WHEN SUM(CASE WHEN uf = 'AL' THEN 1 ELSE 0 END) > SUM(CASE WHEN uf = 'SC' THEN 1 ELSE 0 END) THEN 'ALAGOAS' ELSE 'SANTA CATARINA' END AS estado_mais_casos
FROM dados_covid
GROUP BY EXTRACT(YEAR FROM data_atendimento);
'''

# Executar a consulta SQL e obter os resultados em um DataFrame
df = pd.read_sql_query(consulta, conexao)

# Remover o ".0" do ano
df['ano'] = df['ano'].astype(int)

# Imprimir o DataFrame com os resultados
print(df)

# Encontrar o estado com mais casos
estado_mais_casos = df.loc[df['diferenca_casos'].idxmax(), 'estado_mais_casos']

# Imprimir o estado com mais casos
print(f"O estado com mais casos foi: {estado_mais_casos}")

# Fechar a conexão com o banco de dados
conexao.close()