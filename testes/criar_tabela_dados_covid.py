import psycopg2

string_conexao = "dbname='de' host='localhost' user='postgres' password='postgres'"

conexao = psycopg2.connect(string_conexao)

cursor = conexao.cursor()

sql_criar_tabela = "create table dados_covid(id serial primary key, data_atendimento date, " \
                   "idade integer, sexo char(1), municipio character varying(64), estado character varying(64), " \
                   "uf char(2), classificacao character varying(16), comorbidades text, " \
                   "situacao character varying(32), data_obito date);"

cursor.execute(sql_criar_tabela)

conexao.commit()
