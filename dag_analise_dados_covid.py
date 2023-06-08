from datetime import timedelta, datetime

import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

# conexão detro do airflow
STRING_CONEXAO = "dbname='engdados' host='host.docker.internal' port='5442' user='airflow' password='airflow'"

dag_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 6, 6),
    'retries': 0,
    'retry_delay': timedelta(minutes=59)
}

dag_analise_dados_covid = DAG('dag_analise_dados_covid',
                              default_args=dag_args,
                              description='DAG Análise Dados COVID',
                              schedule_interval='60 * * * *',
                              catchup=False,
                              tags=['trabalho', 'covid', 'engenharia', 'dados']
                              )


def excluir_tabelas_analise_banco():
    conexao = psycopg2.connect(STRING_CONEXAO)
    cursor = conexao.cursor()
    cursor.execute("drop table if exists covid_alagoas")
    conexao.commit()


def obter_porcentagem_mulheres_homens_covid():
    conexao = psycopg2.connect(STRING_CONEXAO)
    df = pd.read_sql("select * from dados_covid", conexao)
    total_dados = len(df.index)

    total_mulheres = df['sexo'].value_counts()['F']
    total_homens = df['sexo'].value_counts()['M']

    print("Total: ", total_dados)
    print("Total mulheres: ", total_mulheres)
    print("Total homens: ", total_homens)
    print("Porcentagem mulheres: ", round(((total_mulheres / total_dados) * 100), 2), "%")
    print("Porcentagem homens: ", round(((total_homens / total_dados) * 100), 2), "%")


def obter_porcentagem_recuperado_obitos_covid():
    conexao = psycopg2.connect(STRING_CONEXAO)
    df = pd.read_sql("select * from dados_covid", conexao)
    total_dados = len(df.index)

    total_recuperados = df['situacao'].value_counts()['RECUPERADO'] + df['situacao'].value_counts()['HOSPITALIZADO'] + df['situacao'].value_counts()['ISOLAMENTO DOMICILIAR']
    total_obitos_covid = df['situacao'].value_counts()['ÓBITO']
    total_obitos_outros = df['situacao'].value_counts()['ÓBITO POR OUTRAS CAUSAS']

    print("Total: ", total_dados)
    print("Total recuperados: ", total_recuperados)
    print("Total óbitos por COVID: ", total_obitos_covid)
    print("Total óbitos por outros motivos: ", total_obitos_outros)
    print("Porcentagem recuperados: ", round(((total_recuperados / total_dados) * 100), 2), "%")
    print("Porcentagem óbitos por COVID: ", round(((total_obitos_covid / total_dados) * 100), 2), "%")
    print("Porcentagem óbitos por outros motivos: ", round(((total_obitos_outros / total_dados) * 100), 2), "%")


def obter_porcentagem_pessoas_covid_com_comorbidades():
    conexao = psycopg2.connect(STRING_CONEXAO)
    df = pd.read_sql("select * from dados_covid", conexao)
    total_dados = len(df.index)

    total_sem_comorbidade = df['comorbidades'].isna().sum()
    total_com_comorbidade = df['comorbidades'].notna().sum()
    print("Total: ", total_dados)
    print("Total de pessoas sem comorbidades: ", total_sem_comorbidade)
    print("Total de pessoas com comorbidades: ", total_com_comorbidade)
    print("Porcentagem de pessoas sem comorbidades: ", round(((total_sem_comorbidade / total_dados) * 100), 2), "%")
    print("Porcentagem de pessoas com comorbidades: ", round(((total_com_comorbidade / total_dados) * 100), 2), "%")


# Tarefa 1
tarefa_1 = PythonOperator(task_id='excluir_tabelas_analise_banco', python_callable=excluir_tabelas_analise_banco, dag=dag_analise_dados_covid)

# Tarefa 2
tarefa_2 = PythonOperator(task_id='obter_porcentagem_mulheres_homens_covid', python_callable=obter_porcentagem_mulheres_homens_covid, dag=dag_analise_dados_covid)

# Tarefa 3
tarefa_3 = PythonOperator(task_id='obter_porcentagem_recuperado_obitos_covid', python_callable=obter_porcentagem_recuperado_obitos_covid, dag=dag_analise_dados_covid)

# Tarefa 4
tarefa_4 = PythonOperator(task_id='obter_porcentagem_pessoas_covid_com_comorbidades', python_callable=obter_porcentagem_pessoas_covid_com_comorbidades, dag=dag_analise_dados_covid)

tarefa_1 >> [tarefa_2, tarefa_3, tarefa_4]
