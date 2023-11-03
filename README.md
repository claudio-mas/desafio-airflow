
# Desafio de Airflow da Indicium

Este desafio é um teste do conhecimento básico sobre Orquestração de Dados em Airflow.

O projeto utiliza o Apache Airflow para criar uma Directed Acyclic Graph (DAG) que realiza a extração de dados de um banco de dados SQLite e grava os resultados em arquivos CSV e TXT.

### Estrutura do Código

O código principal está no arquivo desafio_dag.py. Ele importa as bibliotecas necessárias, define os argumentos padrão para os operadores do Airflow e define as tasks do pipeline.

### Importações
O código importa várias bibliotecas, incluindo airflow, sqlite3, csv e pandas.

As bibliotecas estão listadas no arquivo requirements.txt e podem ser instaladas através do pip:

pip install -r requirements.txt

Para conferir se todas as bibliotecas foram instaladas corretamente, utilize pip list e valide as bibliotecas e suas respectivas versões listadas.

### Argumentos Padrão
Os argumentos padrão para os operadores do Airflow são definidos em default_args. Isso inclui informações como o proprietário da DAG, configurações de e-mail e configurações de tentativas.

### Função extract_data
A função extract_data é definida para extrair dados de um banco de dados SQLite. Ela obtém a conexão com o banco de dados, cria um cursor e define uma consulta SQL para selecionar todos os dados da tabela 'Order'.

### Função calculate_quantity_sum
Esta função é responsável por calcular a soma da quantidade de itens em pedidos para o Rio de Janeiro. Ela utiliza a biblioteca pandas para manipular os dados e realizar a soma. O resultado é gravado no arquivo 'count.txt'.

### Função export_final_answer
A função export_final_answer é responsável por exportar a resposta final, em formato codificado em base64, para um arquivo TXT.

### Como Usar
Para usar este projeto, você precisará ter o Apache Airflow instalado e configurado em seu ambiente. Em seguida, você pode adicionar o arquivo desafio_dag.py ao seu diretório de DAGs do Airflow.

A DAG e os operadores do Airflow estão configurados para usar as funções definidas.

Se você já tem o Apache Airflow instalado e configurado, e o arquivo desafio_dag.py está no diretório de DAGs do Airflow, você deve ser capaz de ver a DAG no painel do Airflow e executá-la a partir daí.