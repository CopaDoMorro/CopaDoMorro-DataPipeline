import os
import sys
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
        df.to_csv('./bucket/temp_data.csv', index=False, encoding='utf-8')

    except:

        print("Erro ao armazenar dados em bucket temporário! Verifique os Logs para entender o ocorrido...")
        sys.exit(1)

# Argumentos padrão da DAG.
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

# Definição da DAG.
with DAG('ETL_CopaDoMorro',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    start = DummyOperator(
        task_id='pipeline_start'
    )

    # Tarefa para acessar o banco.
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    end = DummyOperator(
        task_id='pipeline_end'
    )

    # Definir a ordem de execução das tarefas
    start >> extract_task
    extract_task >> end
