from datetime import timedelta, datetime

from airflow import DAG

import psycopg2

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# DAG ARGS
default_args = {
    'owner': 'Teste',
    'start_date': datetime(2023, 6, 3),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

# DAG
dag_teste = DAG('dag_teste',
                default_args=default_args,
                description='DAG de teste',
                schedule_interval='* * * * *',
                catchup=False,
                tags=['trabalho, teste']
                )


def criar_tabela_dados_covid():

    # conexão detro do airflow
    #string_conexao = "dbname='engdados' host='host.docker.internal' port='5442' user='airflow' password='airflow'"
    string_conexao = "dbname='de' host='localhost' user='postgres' password='postgres'"

    conexao = psycopg2.connect(string_conexao)

    cursor = conexao.cursor()

    sql_criar_tabela = "create table dados_covid(id serial primary key, data_atendimento date, " \
                       "idade integer, sexo char(1), municipio character varying(64), estado character varying(64), " \
                       "uf char(2), classificacao character varying(16), comorbidades text, " \
                       "situacao character varying(32), data_obito date);"

    cursor.execute(sql_criar_tabela)

    conexao.commit()


# Tarefa 1
tarefa_1 = EmptyOperator(task_id='teste_inicio', dag=dag_teste)

# Tarefa 2
tarefa_2 = PythonOperator(task_id='criar_tabela_dados_covid', python_callable=criar_tabela_dados_covid, dag=dag_teste)

# Tarefa 3
tarefa_3 = EmptyOperator(task_id='teste_fim', dag=dag_teste)

tarefa_1 >> tarefa_2 >> tarefa_3
