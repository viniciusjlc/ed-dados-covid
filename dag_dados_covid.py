from datetime import timedelta, datetime

from airflow import DAG

import psycopg2
import pandas as pd

from sqlalchemy import create_engine

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# conexão detro do airflow
STRING_CONEXAO = "dbname='engdados' host='host.docker.internal' port='5442' user='airflow' password='airflow'"
STRING_CONEXAO_SQLALCHEMY = "postgresql://airflow:airflow@host.docker.internal:5442/engdados"

dag_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 6, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag_teste = DAG('dag_dados_covid',
                default_args=dag_args,
                description='DAG Dados COVID',
                schedule_interval='15 * * * *',
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


# Tarefa 1
tarefa_1 = PythonOperator(task_id='limpar_banco', python_callable=limpar_banco, dag=dag_teste)

# Tarefa 2
tarefa_2 = PythonOperator(task_id='criar_tabela_dados_covid', python_callable=criar_tabela_dados_covid, dag=dag_teste)

# Tarefa 3
tarefa_3 = PythonOperator(task_id='obter_dados_covid_alagoas', python_callable=obter_dados_covid_alagoas, dag=dag_teste)

# Tarefa 4
tarefa_4 = PythonOperator(task_id='obter_dados_covid_santa_catarina', python_callable=obter_dados_covid_santa_catarina, dag=dag_teste)

# Tarefa 5
tarefa_5 = EmptyOperator(task_id='tratar_dados_alagoas', dag=dag_teste)

# Tarefa 6
tarefa_6 = EmptyOperator(task_id='tratar_dados_santa_catarina', dag=dag_teste)

tarefa_1 >> tarefa_2 >> [tarefa_3, tarefa_4]
tarefa_3 >> tarefa_5
tarefa_4 >> tarefa_6
