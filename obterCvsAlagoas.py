import psycopg2
import pandas as pd
from sqlalchemy import create_engine

conn_string = 'postgresql://postgres:postgres@localhost:5432/de'

db = create_engine(conn_string)
conn = db.connect()

data = pd.read_csv('dados/dados_abertos_maceio_covid.csv', encoding="ISO-8859-1", sep=';', low_memory=False)

df = pd.DataFrame(data)
df.to_sql('covid_maceio', con=conn, if_exists='replace', index=True)
conn = psycopg2.connect(conn_string)
conn.autocommit = True
cursor = conn.cursor()

sql1 = '''select * from covid_maceio;'''
cursor.execute(sql1)
for i in cursor.fetchall():
	print(i)

conn.close()
