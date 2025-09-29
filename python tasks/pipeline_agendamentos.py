from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import pandas as pd
import os

mysql_conn_id= 'mysql_default'

def ultima_data_ingestao(**kwargs):
    mysql_hook= MySqlHook(mysql_conn_id= mysql_conn_id)
    resultado = mysql_hook.get_first('select max(data_ingestao) from agendamentos')
    data_ultima_ingestao = resultado[0] if resultado and resultado[0] else datetime(2020,1,1)
    kwargs['ti'].xcom_push(key='data_ultima_ingestao', value= data_ultima_ingestao)
    print(f'Última atualização: {data_ultima_ingestao}')

def extracao_transformacao(**kwargs):
    ti=kwargs['ti']
    ultima_data_ingestao = ti.xcom_pull(task_ids='encontre_ultima_atualizacao',key='data_ultima_ingestao')
    file_path='/opt/airflow/dags/agenda.csv'
    df= pd.read_csv(file_path)
    nomes_colunas = {'CNPJ_CPF': 'cpf', 'NOME': 'nome', 'DATA': 'data_consulta', 'HORA': 'hora', 'RESPONSAVEL': 'responsavel', 'FALTOU': 'faltou', 'OBS': 'procedimento','FICHA': 'prontuario', 'DEPARTAMENTO': 'especialidade'}
    df.rename(columns=nomes_colunas, inplace=True)
    df['data_consulta'] = pd.to_datetime(df['data_consulta'])
    if ultima_data_ingestao == datetime(2020, 1, 1):
        df_novo = df.copy()
    else:
        df_novo = df[df['data_consulta'] > pd.to_datetime(ultima_data_ingestao)]
    colunas_a_remover = ['MODIFICADO', 'ID_AGENDA', 'DATA_EXC', 'AVISO', 'COLUNA', 'LINHA', 'TERMINAL', 'LANCTO','FONE_1', 'FONE_2', 'USUARIO_EXC', 'CODIGO_ESPECIALIDADE', 'CONFIRMACAO_DATA_HORA','CONFIRMACAO_USUARIO', 'CONFIRMACAO_OBSERVACAO', 'DT_AXON', 'AXON_ID', 'ENCAIXE', 'CODIGO_RESP', 'USUARIO']
    df_novo.drop(columns=colunas_a_remover, inplace=True, errors='ignore')
    df_novo.drop(columns=colunas_a_remover,inplace=True,errors='ignore')
    df_novo.drop(columns=colunas_a_remover,inplace=True,errors='ignore')
    df_novo['cpf'].fillna('nao informado',inplace=True)
    df_novo['faltou'].fillna('N', inplace=True)
    df_novo['procedimento'].fillna('sem observacao', inplace=True)
    df_novo['prontuario'].fillna(0, inplace=True)
    
    if not df_novo.empty:
        df_novo.to_csv(f"{os.environ['AIRFLOW_HOME']}/logs/agendamentos_novos.csv", index=False)
        print(f"Novos registros encontrados: {len(df_novo)}")
    else:
        print("Nenhum registro novo encontrado.")
    
def load_novos_dados(**kwargs):
        file_path= f"{os.environ['AIRFLOW_HOME']}/logs/agendamentos_novos.csv"
        if not os.path.exists(file_path):
            print('Sem dados novos para carregar.')
            return
        df= pd.read_csv(file_path)
        mysql_hook= MySqlHook(mysql_conn_id= mysql_conn_id)

        try:
            df.to_sql(name='agendamentos',con=mysql_hook.get_sqlalchemy_engine(),
                      if_exists='append',index=False)
            print(f'{len(df)} Novos dados carregados!')
        except Exception as e:
            print(f'Erro ao carregar dados:{e}')

with DAG(
    dag_id='pipeline_consultas_agendadas',
    start_date=datetime(2020,1,1),
    catchup=True,
    tags=['ingestao','consultas'],
) as dag:
    encontrar_ultima_atualizacao= PythonOperator(task_id='encontre_ultima_atualizacao',python_callable=ultima_data_ingestao,)
    extrair_transformar= PythonOperator(task_id='extrair_transformar',python_callable=extracao_transformacao,)
    carregar_dados= PythonOperator(task_id='carregar_dados_novos', python_callable=load_novos_dados,)

    encontrar_ultima_atualizacao>>extrair_transformar>>carregar_dados
 