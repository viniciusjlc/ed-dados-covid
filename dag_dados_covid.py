from datetime import timedelta, datetime

from airflow import DAG

import psycopg2
import pandas as pd
import numpy as np

from sqlalchemy import create_engine

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# conexão detro do airflow
STRING_CONEXAO = "dbname='engdados' host='host.docker.internal' port='5442' user='airflow' password='airflow'"
STRING_CONEXAO_SQLALCHEMY = "postgresql://airflow:airflow@host.docker.internal:5442/engdados"

dag_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 6, 6),
    'retries': 0,
    'retry_delay': timedelta(minutes=60)
}

dag_teste = DAG('dag_dados_covid_2',
                default_args=dag_args,
                description='DAG Dados COVID',
                schedule_interval='60 * * * *',
                catchup=False,
                tags=['trabalho', 'covid', 'engenharia', 'dados']
                )


def limpar_banco():
    conexao = psycopg2.connect(STRING_CONEXAO)
    cursor = conexao.cursor()
    cursor.execute("drop table if exists covid_alagoas")
    cursor.execute("drop table if exists covid_santa_catarina")
    cursor.execute("drop table if exists dados_covid")
    conexao.commit()


def obter_dados_covid_alagoas():
    db = create_engine(STRING_CONEXAO_SQLALCHEMY)
    conexao = db.connect()

    data = pd.read_csv('/opt/airflow/dags/dados/dados_abertos_maceio_covid.csv', encoding="ISO-8859-1", sep=';', low_memory=False)

    df = pd.DataFrame(data)
    df.to_sql('covid_alagoas', con=conexao, if_exists='replace', index=True)

    conexao = psycopg2.connect(STRING_CONEXAO_SQLALCHEMY)
    conexao.autocommit = True
    cursor = conexao.cursor()

    cursor.execute('''select * from covid_alagoas;''')
    conexao.close()


def obter_dados_covid_santa_catarina():
    db = create_engine(STRING_CONEXAO_SQLALCHEMY)
    conexao = db.connect()

    data = pd.read_csv('/opt/airflow/dags/dados/dados_abertos_boavista_covid.csv', encoding="ISO-8859-1", sep=';', low_memory=False)

    df = pd.DataFrame(data)
    df.to_sql('covid_santa_catarina', con=conexao, if_exists='replace', index=True)

    conexao = psycopg2.connect(STRING_CONEXAO_SQLALCHEMY)
    conexao.autocommit = True
    cursor = conexao.cursor()

    cursor.execute('''select * from covid_santa_catarina;''')

    conexao.close()


def criar_tabela_dados_covid():
    conexao = psycopg2.connect(STRING_CONEXAO)

    cursor = conexao.cursor()

    sql_criar_tabela = "create table dados_covid(id serial primary key, data_atendimento date, " \
                       "idade integer, sexo char(1), municipio character varying(64), estado character varying(64), " \
                       "uf char(2), classificacao character varying(16), comorbidades text, " \
                       "situacao character varying(32), data_obito date);"

    cursor.execute(sql_criar_tabela)

    conexao.commit()


def tratar_dados_alagoas():
    conexao = psycopg2.connect(STRING_CONEXAO)

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
          "from covid_alagoas " \
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

    # tratando data_obito
    dataFrame['data_obito'] = dataFrame['data_obito'].replace({np.nan: None})

    # tratando situacao
    dataFrame['situacao'] = dataFrame['situacao'].str.strip()
    dataFrame['situacao'] = dataFrame['situacao'].str.upper()

    # tratando municipios
    dataFrame['municipio'] = dataFrame['municipio'].str.strip()
    dataFrame['municipio'] = dataFrame['municipio'].str.upper()

    print(dataFrame[['data_atendimento', 'idade', 'sexo', 'comorbidades', 'data_obito']].head(10))

    colunas = ",".join([str(i) for i in dataFrame.columns.tolist()])
    print("INICIO INSERT")
    for i, row in dataFrame.iterrows():
        sql = "insert into dados_covid (" + colunas + ") values (" + "%s," * (len(row) - 1) + "%s)"
        cursor.execute(sql, tuple(row))

    conexao.commit()
    print("FIM INSERT")


def tratar_dados_santa_catarina():
    conexao = psycopg2.connect(STRING_CONEXAO)

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
          "from covid_boavista " \
          "order by data_atendimento"

    dataFrame = pd.read_sql(sql, conexao)

    # tratando data_atendimento
    dataFrame['data_atendimento'] = dataFrame['data_atendimento'].replace(r'[a-zA-Z]', np.nan, regex=True)
    dataFrame = dataFrame.dropna(subset=['data_atendimento'])
    dataFrame['data_atendimento'] = pd.to_datetime(dataFrame['data_atendimento'], format="%d/%m/%Y")

    # tratando idade
    dataFrame = dataFrame.dropna(subset=['idade'])
    dataFrame['idade'] = dataFrame['idade'].apply(np.int64)

    # tratando sexo
    dataFrame['sexo'] = dataFrame['sexo'].replace('FEMININO', 'F', regex=True)
    dataFrame['sexo'] = dataFrame['sexo'].replace('MASCULINO', 'M', regex=True)
    dataFrame['sexo'] = dataFrame['sexo'].replace('NAO INFORMADO', np.nan, regex=True)
    dataFrame = dataFrame.dropna(subset=['sexo'])

    # tratando situacao
    dataFrame['situacao'] = dataFrame['situacao'].replace('SIM', 'RECUPERADO', regex=True)
    dataFrame['situacao'] = dataFrame['situacao'].replace('NÃO', 'ÓBITO', regex=True)
    dataFrame['situacao'] = dataFrame['situacao'].replace('NAO', 'ÓBITO', regex=True)

    colunas = ",".join([str(i) for i in dataFrame.columns.tolist()])
    print("INICIO INSERT")
    for i, row in dataFrame.iterrows():
        sql = "insert into dados_covid (" + colunas + ") values (" + "%s," * (len(row) - 1) + "%s)"
        cursor.execute(sql, tuple(row))

    conexao.commit()
    print("FIM INSERT")


# Tarefa 1
tarefa_1 = PythonOperator(task_id='limpar_banco', python_callable=limpar_banco, dag=dag_teste)

# Tarefa 2
tarefa_2 = PythonOperator(task_id='criar_tabela_dados_covid', python_callable=criar_tabela_dados_covid, dag=dag_teste)

# Tarefa 3
tarefa_3 = PythonOperator(task_id='obter_dados_covid_alagoas', python_callable=obter_dados_covid_alagoas, dag=dag_teste)

# Tarefa 4
tarefa_4 = PythonOperator(task_id='obter_dados_covid_santa_catarina', python_callable=obter_dados_covid_santa_catarina, dag=dag_teste)

# Tarefa 5
tarefa_5 = PythonOperator(task_id='tratar_dados_alagoas', python_callable=tratar_dados_alagoas, dag=dag_teste)

# Tarefa 6
tarefa_6 = PythonOperator(task_id='tratar_dados_santa_catarina', python_callable=tratar_dados_santa_catarina, dag=dag_teste)

tarefa_1 >> tarefa_2 >> [tarefa_3, tarefa_4]
tarefa_3 >> tarefa_5
tarefa_4 >> tarefa_6
