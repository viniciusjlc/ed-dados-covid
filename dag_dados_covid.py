from datetime import timedelta, datetime

import numpy as np
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine

# conexão detro do airflow
STRING_CONEXAO = "dbname='engdados' host='host.docker.internal' port='5442' user='airflow' password='airflow'"
STRING_CONEXAO_SQLALCHEMY = "postgresql://airflow:airflow@host.docker.internal:5442/engdados"

dag_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 6, 6),
    'retries': 0,
    'retry_delay': timedelta(minutes=60)
}

dag_dados_covid = DAG('dag_dados_covid',
                      default_args=dag_args,
                      description='DAG Dados COVID',
                      schedule_interval='59 * * * *',
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

    data = pd.read_csv('/opt/airflow/dags/dados/dados_abertos_maceio_covid.csv', encoding="ISO-8859-1", sep=';',
                       low_memory=False)

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

    data = pd.read_csv('/opt/airflow/dags/dados/dados_abertos_boavista_covid.csv', encoding="ISO-8859-1", sep=';',
                       low_memory=False)

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
          "where \"Data de atendimento no Serviço\"  >= '2020-02-20' " \
          "and \"Data de atendimento no Serviço\"  <= '2023-01-01'" \
          "order by data_atendimento"

    df = pd.read_sql(sql, conexao)

    # tratando idade
    df = df[df.idade.apply(lambda valor: valor.isnumeric())]

    # tratando sexo
    df['sexo'] = df['sexo'].str.strip()
    df['sexo'] = df['sexo'].str.upper()

    # tratando comorbidades
    df['comorbidades'] = df['comorbidades'].str.strip()
    df['comorbidades'] = df['comorbidades'].str.upper()
    df['comorbidades'] = df['comorbidades'].replace('/', ',', regex=True)
    df['comorbidades'] = df['comorbidades'].replace('SEM COMORBIDADE', None, regex=True)
    df['comorbidades'] = df['comorbidades'].replace('SEM INFORMAÇÃO', None, regex=True)
    df['comorbidades'] = df['comorbidades'].replace('RECUPERADO', None, regex=True)

    # tratando data_obito
    df['data_obito'] = df['data_obito'].replace({np.nan: None})

    # tratando situacao
    df['situacao'] = df['situacao'].str.strip()
    df['situacao'] = df['situacao'].str.upper()

    # tratando municipios
    df['municipio'] = df['municipio'].str.strip()
    df['municipio'] = df['municipio'].str.upper()

    print(df[['data_atendimento', 'idade', 'sexo', 'comorbidades', 'data_obito']].head(10))

    colunas = ",".join([str(i) for i in df.columns.tolist()])
    print("INICIO INSERT")
    for i, row in df.iterrows():
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
          "from covid_santa_catarina " \
          "where data_coleta  >= '2020-02-20' " \
          "and data_coleta  <= '2023-01-01'" \
          "order by data_atendimento"

    df = pd.read_sql(sql, conexao)

    # tratando data_atendimento
    df['data_atendimento'] = df['data_atendimento'].replace(r'[a-zA-Z]', np.nan, regex=True)
    df = df.dropna(subset=['data_atendimento'])
    df['data_atendimento'] = pd.to_datetime(df['data_atendimento'], format="%d/%m/%Y")

    # tratando idade
    df = df.dropna(subset=['idade'])
    df['idade'] = df['idade'].apply(np.int64)

    # tratando sexo
    df['sexo'] = df['sexo'].replace('FEMININO', 'F', regex=True)
    df['sexo'] = df['sexo'].replace('MASCULINO', 'M', regex=True)
    df['sexo'] = df['sexo'].replace('NAO INFORMADO', np.nan, regex=True)
    df = df.dropna(subset=['sexo'])

    # tratando situacao
    df['situacao'] = df['situacao'].replace('SIM', 'RECUPERADO', regex=True)
    df['situacao'] = df['situacao'].replace('NÃO', 'ÓBITO', regex=True)
    df['situacao'] = df['situacao'].replace('NAO', 'ÓBITO', regex=True)

    colunas = ",".join([str(i) for i in df.columns.tolist()])
    print("INICIO INSERT")
    for i, row in df.iterrows():
        sql = "insert into dados_covid (" + colunas + ") values (" + "%s," * (len(row) - 1) + "%s)"
        cursor.execute(sql, tuple(row))

    conexao.commit()
    print("FIM INSERT")


# Tarefa 1
tarefa_1 = PythonOperator(task_id='limpar_banco', python_callable=limpar_banco, dag=dag_dados_covid)

# Tarefa 2
tarefa_2 = PythonOperator(task_id='criar_tabela_dados_covid', python_callable=criar_tabela_dados_covid,
                          dag=dag_dados_covid)

# Tarefa 3
tarefa_3 = PythonOperator(task_id='obter_dados_covid_alagoas', python_callable=obter_dados_covid_alagoas,
                          dag=dag_dados_covid)

# Tarefa 4
tarefa_4 = PythonOperator(task_id='obter_dados_covid_santa_catarina', python_callable=obter_dados_covid_santa_catarina,
                          dag=dag_dados_covid)

# Tarefa 5
tarefa_5 = PythonOperator(task_id='tratar_dados_alagoas', python_callable=tratar_dados_alagoas, dag=dag_dados_covid)

# Tarefa 6
tarefa_6 = PythonOperator(task_id='tratar_dados_santa_catarina', python_callable=tratar_dados_santa_catarina,
                          dag=dag_dados_covid)

tarefa_1 >> tarefa_2 >> [tarefa_3, tarefa_4]
tarefa_3 >> tarefa_5
tarefa_4 >> tarefa_6
