import os
import sys
import uuid
import pandas as pd
from airflow import DAG
from sqlalchemy import create_engine
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Função para realizar o Extract do ETL.
def extract():
    
    db_endpoint = "postgresql+psycopg2://postgres.njtzfnefftlljglzpymt:copadomorro@aws-0-sa-east-1.pooler.supabase.com/postgres"

    try:

        engine = create_engine(f'{db_endpoint}')

    except:

        print("Erro ao acessar o Banco de Dados! Verifique a conexão para continuar...")
        sys.exit(1)


    query = 'SELECT * FROM be.dw'

    try:
        
        with engine.connect() as conn:

            df = pd.read_sql(sql=query, con=conn.connection)

    except:

        print("Erro ao extrair dados para DataFrame! Verifique os Logs para entender o ocorrido...")
        sys.exit(1)

    try:

        os.mkdir(os.path.join(os.getcwd(), 'bucket'))
        df.to_csv('./bucket/temp_data.csv', index=False, encoding='utf-8', sep=',', mode='w+')

    except:

        print("Erro ao armazenar dados em bucket temporário! Verifique os Logs para entender o ocorrido...")
        sys.exit(1)

# Argumentos padrão da DAG.
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

# Função para realizar o Transform do ETL.
def transform():

    def gerar_uuid():

        return str(uuid.uuid4())

    try:

        df = pd.read_csv('./bucket/temp_data.csv', sep=',', encoding='utf-8')

    except:

        print("Erro ao abrir os dados salvos no bucket! Verifique os Logs para entender o ocorrido...")
        sys.exit(1)
    
    try:

        df['id_jogador'] = df.apply(lambda row: gerar_uuid(), axis=1)
        df['id_responsavel'] = df.apply(lambda row: gerar_uuid(), axis=1)
        df['id_jogo'] = df.apply(lambda row: gerar_uuid(), axis=1)

    except:

        print("Erro ao gerar os UUIDs da Tabela Fato! Verifique os Logs para entender o ocorrido...")
        sys.exit(1)

    try:

        df_responsavel = df.loc[:, ['id_responsavel', 'nome_responsavel', 'cpf_responsavel', 'email_responsavel']]
        df_jogo = df.loc[:, ['id_jogo', 'nome_jogo', 'data_jogo', 'estado_jogo', 'cidade_jogo', 'comunidade_jogo']]
        df_jogador = df.loc[:, ['id_jogador', 'nome_jogador', 'cpf_jogador', 'data_nascimento_jogador', 'estado_jogador', 'cidade_jogador', 'comunidade_jogador', 'cpf_responsavel']]
        df_jogo_jogador = df.loc[:, ['id_responsavel', 'id_jogo', 'id_jogador']]

    except:

        print('Erro ao gerar as tabelas dimensão do Data Warehouse! Verifique os Logs para entender o ocorrido...')
        sys.exit(1)
    
    try:

        df_responsavel.to_csv('./bucket/dim_responsavel.csv', index=False, encoding='utf-8', sep=',', mode='w+')
        df_jogo.to_csv('./bucket/dim_jogo.csv', index=False, encoding='utf-8', sep=',', mode='w+')
        df_jogador.to_csv('./bucket/dim_jogador.csv', index=False, encoding='utf-8', sep=',', mode='w+')
        df_jogo_jogador.to_csv('./bucket/fato_jogo_jogador.csv', index=False, encoding='utf-8', sep=',', mode='w+')
    
    except:

        print("Erro ao armazenar dados transformados em bucket temporário! Verifique os Logs para entender o ocorrido...")
        sys.exit(1)
        
# Definição da DAG.
with DAG('ETL_CopaDoMorro',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    start = DummyOperator(
        task_id='pipeline_start'
    )

    # Tarefa para extrair os dados.
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    # Tarefa para transformar os dados.
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    end = DummyOperator(
        task_id='pipeline_end'
    )

    # Definir a ordem de execução das tarefas
    start >> extract_task
    extract_task >> transform_task
    transform_task >> end
