# Pipeline ETL Automatizado com Airflow e MySQL

Este projeto demonstra a construção de um pipeline ETL (Extração, Transformação e Carga) completo, projetado para automatizar a ingestão de dados a partir de um arquivo-fonte para um banco de dados relacional.
A solução foi desenvolvida para ser escalável e confiável, utilizando ferramentas padrão do mercado para orquestração e armazenamento de dados.

## Tecnologias Utilizadas
- **Orquestração:** Apache Airflow. Agenda, monitora e gerenciara e orquestra o fluxo das tasks.
- **Contêineres:** Docker
- **Linguagem:** Python, SQL
- **Banco de Dados:** MySQL
- **Bibliotecas:** Pandas

O pipeline foi orquestrado pelo Apache Airflow, rodando em um ambiente Docker para garantir a portabilidade e um ambiente de execução isolado. A DAG em Python automatiza cada etapa do processo, conectando-se a uma instância local de MySQL na minha máquina para gerenciar o banco de dados de destino.

  - **Extração (Extract):** O pipeline se conecta a um diretório e extrai dados de um arquivo-fonte (CSV).

  - **Transformação (Transform):** Utilizando a biblioteca Pandas, os dados brutos são preparados para a ingestão. As operações de transformação incluem: Renomear colunas para garantir a padronização e facilitar a análise,,anipular valores ausentes (nulos) para evitar inconsistências,remover colunas desnecessárias, otimizando o volume de dados a ser processado. e converter tipos de dados para assegurar a compatibilidade com o esquema do banco de dados.

- **Carga (Load):** Os dados transformados são carregados na tabela MySQL. A lógica de carga é híbrida, o que permite ao pipeline realizar uma carga completa de todo o histórico na primeira execução (quando a tabela está vazia),fazer cargas incrementais subsequentes, processando e inserindo apenas os novos registros a cada execução.

O resultado é um processo de ingestão de dados automatizado e robusto, capaz de garantir a consistência e a disponibilidade de dados de forma eficiente.


## Fonte dos dados

Os dados utilizados neste projeto se originam de um arquivo-fonte em formato CSV. Esse arquivo contém um vasto histórico de agendamentos, com um total de 2.006.276 registros, que abrangem o período de 2020 até o presente.

Carga Inicial: Na primeira execução, o pipeline realiza uma carga completa, importando todos os registros históricos desde 2020.

Cargas Subsequentes: Em execuções diárias, ele muda para o modo incremental, processando e inserindo apenas os novos dados que surgirem no arquivo.

## Arquitetura do Projeto

- **encontre_ultima_atualizacao:** Conecta ao MySQL para descobrir a data do último registro carregado, permitindo a ingestão incremental.
- **extrair_transformar:** Lê o arquivo CSV e aplica as regras de negócio: renomeia colunas, remove dados irrelevantes e trata valores nulos.
- **carregar_dados_novos:** Carrega os dados transformados na tabela do MySQL.

![3](https://github.com/user-attachments/assets/874f209d-160c-43d6-b3e8-a5a593677081)

![4](https://github.com/user-attachments/assets/02a11b09-bdd2-4cdd-b264-2eb483dd8581)
![7](https://github.com/user-attachments/assets/d800c547-ecac-4524-a3ef-971362e57728)

## Tasks

A DAG é composta por três tarefas, cada uma com uma função específica na pipeline de ETL:

 - **ultima_data_ingestao:** Esta função se conecta ao banco de dados MySQL para descobrir a data mais recente do último registro que foi carregado. Essa informação é crucial para a nossa lógica de carga híbrida, pois nos permite saber de onde começar a busca por novos dados.
 - **extracao_transformacao:** A partir da data obtida na tarefa anterior, esta função usa a biblioteca Pandas para ler o arquivo CSV e filtrar apenas os registros mais novos. Além de extrair, ela transforma os dados, padronizando nomes de colunas, tratando valores nulos e removendo colunas que não serão usadas na análise.
 - **load_novos_dados:** Esta é a última etapa. A função pega os dados já limpos e transformados e os carrega na tabela do MySQL. A cada execução, ela verifica se há novos registros e, em caso positivo, os insere no banco de dados, garantindo que a nossa tabela esteja sempre atualizada.

## Ingestão de Dados

O banco de dados foi criado no MySQL para servir como o destino final das informações. Dentro dele, uma tabela chamada agendamentos foi desenvolvida para armazenar os dados extraídos do arquivo CSV, com colunas e tipos de dados definidos para otimizar o processo de carga e análise.

![criação db](https://github.com/user-attachments/assets/fb18e9c9-b040-46f0-b982-612d06afd1c3)

![5](https://github.com/user-attachments/assets/c27b12a9-051e-4fe0-b466-02cef3edd35b)






