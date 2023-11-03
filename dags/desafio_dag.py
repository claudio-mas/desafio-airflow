from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import airflow.models
from airflow.models import Variable

from datetime import datetime, timedelta
import os

import sqlite3
import csv
import pandas as pd


# Args passados para os operadores
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['claudio.melo@indicium.tech'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

current_dir = os.getcwd()
file_path = os.path.join(current_dir, "dags/data/Northwind_small.sqlite")

# Task (1) para ler os dados da tabela 'Order' do banco de dados Northwind_small
# e gravar o arquivo "output_orders.csv".
def extract_data():
    # Conectar ao banco de dados
    # conn = sqlite3.connect('/home/claudiomelo/airflow-data/dags/data/Northwind_small.sqlite')
    conn = sqlite3.connect(file_path)
    cursor = conn.cursor()

    # Consulta SQL para selecionar todos os dados da tabela 'Order'
    query = "SELECT * FROM 'Order'"

    # Executar a consulta e recuperar os dados
    cursor.execute(query)
    orders_data = cursor.fetchall()

    # Fechar a conexão com o banco de dados
    conn.close()

    # Escrever os dados em um arquivo CSV
    with open('output_orders.csv', 'w', newline='') as file:
        csv_writer = csv.writer(file)
        # Escrever o cabeçalho com os nomes das colunas
        csv_writer.writerow([description[0] for description in cursor.description])
        # Escrever os dados
        csv_writer.writerows(orders_data)
        

# Task (2) para ler os dados da tabela "OrderDetail" do mesmo banco de dados Northwind
# e fazer um 'JOIN' com o arquivo "output_orders.csv" da primeira task;
# calcula a soma da quantidade vendida com destino para o Rio de Janeiro;
# exporta o resultado para o arquivo "count.txt".
def calculate_quantity_sum():
    # Abre o arquivo CSV
    with open("output_orders.csv", "r") as f:
        # Cria um leitor CSV
        reader = csv.reader(f)
        # Pula o cabeçalho
        next(reader, None)
        # Inicializa um contador
        count = 0

        # Percorre os dados do arquivo CSV
        for row in reader:
            # Obtém o ID do pedido
            order_id = int(row[0])
            # Conecta-se ao banco de dados
            # conn = sqlite3.connect("/home/claudiomelo/airflow-data/dags/data/Northwind_small.sqlite")
            conn = sqlite3.connect(file_path)
            # Seleciona os dados da tabela 'OrderDetail'
            cur = conn.cursor()
            cur.execute(
                "SELECT Quantity FROM OrderDetail WHERE OrderID = ?",
                (order_id,),
            )
            # Obtém os dados da tabela 'OrderDetail'
            for row_detail in cur:
                # Verifica se o destino é o Rio de Janeiro
                if row[10] == "Rio de Janeiro":
                    # Adiciona a quantidade vendida ao contador
                    count += int(row_detail[0])

            # Fecha a conexão com o banco de dados
            conn.close()

    # Escreve o contador no arquivo "count.txt"
    with open("count.txt", "w") as f:
        f.write(str(count))

# exporta o resultado para um arquivo em um formato codificado em base64
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None

# Definir o DAG
with DAG(
    'desafio_airflow_LH',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=None,
    start_date=datetime(2023, 11, 3),
    catchup=False,
    tags=['desafio'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
        Cláudio Más de Melo - LH 2023.10
    """

# Definir as tasks
extract_task_1 = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data,
    dag=dag
)

calculate_task_2 = PythonOperator(
    task_id='calculate_quantity_sum_task',
    python_callable=calculate_quantity_sum,
    dag=dag
)

export_final_output = PythonOperator(
    task_id='export_final_output',
    python_callable=export_final_answer,
    provide_context=True
)

# Definir as dependências entre as tasks
extract_task_1 >> calculate_task_2 >> export_final_output
