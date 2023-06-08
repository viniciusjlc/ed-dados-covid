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
    'retry_delay': timedelta(minutes=60)
}

dag_analise_dados_covid = DAG('dag_analise_dados_covid',
                              default_args=dag_args,
                              description='DAG Análise Dados COVID',
                              schedule_interval='59 * * * *',
                              catchup=False,
                              tags=['trabalho', 'covid', 'engenharia', 'dados']
                              )


def excluir_tabelas_estatisticas_covid_banco():
    conexao = psycopg2.connect(STRING_CONEXAO)
    cursor = conexao.cursor()
    cursor.execute("drop table if exists est_covid_genero")
    cursor.execute("drop table if exists est_covid_recuperados_obito")
    cursor.execute("drop table if exists est_covid_comorbidades")
    conexao.commit()


def criar_tabelas_estatisticas_covid_banco():
    conexao = psycopg2.connect(STRING_CONEXAO)
    cursor = conexao.cursor()

    sql_criar_tabela_est_covid_genero = "create table est_covid_genero(" \
                                        "id serial primary key, " \
                                        "sexo char(1), " \
                                        "quantidate bigint, " \
                                        "porcentagen double precision);"
    cursor.execute(sql_criar_tabela_est_covid_genero)

    sql_criar_tabela_est_covid_recuperados_obito = "create table est_covid_recuperados_obito(" \
                                                   "id serial primary key, " \
                                                   "situacao character varying(32), " \
                                                   "quantidate bigint, " \
                                                   "porcentagen double precision);"
    cursor.execute(sql_criar_tabela_est_covid_recuperados_obito)

    sql_criar_tabela_est_covid_comorbidades = "create table est_covid_comorbidades(" \
                                              "id serial primary key, " \
                                              "possui_comorbidades char(1), " \
                                              "quantidate bigint, " \
                                              "porcentagen double precision);"
    cursor.execute(sql_criar_tabela_est_covid_comorbidades)

    conexao.commit()


def obter_porcentagem_mulheres_homens_covid():
    conexao = psycopg2.connect(STRING_CONEXAO)
    cursor = conexao.cursor()
    df = pd.read_sql("select * from dados_covid", conexao)
    total_dados = len(df.index)

    total_mulheres = df['sexo'].value_counts()['F']
    total_homens = df['sexo'].value_counts()['M']
    porcentagem_mulheres = round(((total_mulheres / total_dados) * 100), 2)
    porcentagem_homens = round(((total_homens / total_dados) * 100), 2)

    print("Total: ", total_dados)
    print("Total mulheres: ", total_mulheres)
    print("Total homens: ", total_homens)
    print("Porcentagem mulheres: ", porcentagem_mulheres, "%")
    print("Porcentagem homens: ", porcentagem_homens, "%")

    sql = "insert into est_covid_genero (sexo, quantidate, porcentagen) values('{}','{}','{}')".format('F', total_mulheres, porcentagem_mulheres)
    cursor.execute(cursor.mogrify(sql))

    sql = "insert into est_covid_genero (sexo, quantidate, porcentagen) values('{}','{}','{}')".format('M', total_homens, porcentagem_homens)
    cursor.execute(cursor.mogrify(sql))

    conexao.commit()


def obter_porcentagem_recuperado_obitos_covid():
    conexao = psycopg2.connect(STRING_CONEXAO)
    cursor = conexao.cursor()
    df = pd.read_sql("select * from dados_covid", conexao)
    total_dados = len(df.index)

    total_recuperados = df['situacao'].value_counts()['RECUPERADO'] + df['situacao'].value_counts()['HOSPITALIZADO'] + df['situacao'].value_counts()['ISOLAMENTO DOMICILIAR']
    total_obitos_covid = df['situacao'].value_counts()['ÓBITO']
    total_obitos_outros = df['situacao'].value_counts()['ÓBITO POR OUTRAS CAUSAS']
    porcentagem_recuperados = round(((total_recuperados / total_dados) * 100), 2)
    porcentagem_obitos = round(((total_obitos_covid / total_dados) * 100), 2)
    porcentagem_obtos_outros = round(((total_obitos_outros / total_dados) * 100), 2)

    print("Total: ", total_dados)
    print("Total recuperados: ", total_recuperados)
    print("Total óbitos por COVID: ", total_obitos_covid)
    print("Total óbitos por outros motivos: ", total_obitos_outros)
    print("Porcentagem recuperados: ", porcentagem_recuperados, "%")
    print("Porcentagem óbitos por COVID: ", porcentagem_obitos, "%")
    print("Porcentagem óbitos por outros motivos: ", porcentagem_obtos_outros, "%")

    sql = "insert into est_covid_recuperados_obito (situacao, quantidate, porcentagen) values('{}','{}','{}')".format('RECUPERADOS', total_recuperados, porcentagem_recuperados)
    cursor.execute(cursor.mogrify(sql))

    sql = "insert into est_covid_recuperados_obito (situacao, quantidate, porcentagen) values('{}','{}','{}')".format('ÓBITO', total_obitos_covid, porcentagem_obitos)
    cursor.execute(cursor.mogrify(sql))

    sql = "insert into est_covid_recuperados_obito (situacao, quantidate, porcentagen) values('{}','{}','{}')".format('ÓBITO OUTRAS CAUSAS', total_obitos_outros, porcentagem_obtos_outros)
    cursor.execute(cursor.mogrify(sql))

    conexao.commit()


def obter_porcentagem_pessoas_covid_com_comorbidades():
    conexao = psycopg2.connect(STRING_CONEXAO)
    cursor = conexao.cursor()
    df = pd.read_sql("select * from dados_covid", conexao)
    total_dados = len(df.index)

    total_sem_comorbidade = df['comorbidades'].isna().sum()
    total_com_comorbidade = df['comorbidades'].notna().sum()
    porcentagem_total_sem_comorbidade = round(((total_sem_comorbidade / total_dados) * 100), 2)
    porcentagem_total_com_comorbidade = round(((total_com_comorbidade / total_dados) * 100), 2)

    print("Total: ", total_dados)
    print("Total de pessoas sem comorbidades: ", total_sem_comorbidade)
    print("Total de pessoas com comorbidades: ", total_com_comorbidade)
    print("Porcentagem de pessoas sem comorbidades: ", porcentagem_total_sem_comorbidade, "%")
    print("Porcentagem de pessoas com comorbidades: ", porcentagem_total_com_comorbidade, "%")

    sql = "insert into est_covid_comorbidades (possui_comorbidades, quantidate, porcentagen) values('{}','{}','{}')".format('N', total_sem_comorbidade, porcentagem_total_sem_comorbidade)
    cursor.execute(cursor.mogrify(sql))

    sql = "insert into est_covid_comorbidades (possui_comorbidades, quantidate, porcentagen) values('{}','{}','{}')".format('S', total_com_comorbidade, porcentagem_total_com_comorbidade)
    cursor.execute(cursor.mogrify(sql))

    conexao.commit()


# Tarefa 1
tarefa_1 = PythonOperator(task_id='excluir_tabelas_estatisticas_covid_banco',
                          python_callable=excluir_tabelas_estatisticas_covid_banco,
                          dag=dag_analise_dados_covid)

# Tarefa 2
tarefa_2 = PythonOperator(task_id='criar_tabelas_estatisticas_covid_banco',
                          python_callable=criar_tabelas_estatisticas_covid_banco,
                          dag=dag_analise_dados_covid)

# Tarefa 3
tarefa_3 = PythonOperator(task_id='obter_porcentagem_mulheres_homens_covid',
                          python_callable=obter_porcentagem_mulheres_homens_covid,
                          dag=dag_analise_dados_covid)

# Tarefa 4
tarefa_4 = PythonOperator(task_id='obter_porcentagem_recuperado_obitos_covid',
                          python_callable=obter_porcentagem_recuperado_obitos_covid,
                          dag=dag_analise_dados_covid)

# Tarefa 5
tarefa_5 = PythonOperator(task_id='obter_porcentagem_pessoas_covid_com_comorbidades',
                          python_callable=obter_porcentagem_pessoas_covid_com_comorbidades,
                          dag=dag_analise_dados_covid)

tarefa_1 >> tarefa_2 >> [tarefa_3, tarefa_4, tarefa_5]
