import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from sklearn.preprocessing import StandardScaler, MinMaxScaler
import gspread
from oauth2client.service_account import ServiceAccountCredentials

CREDENTIALS_PATH = "/opt/airflow/dags/credentials/google_sheets_key.json"

def load_from_google_sheets(**kwargs):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
    client = gspread.authorize(creds)
    sheet = client.open('ModelDataset').sheet1
    data = sheet.get_all_records()
    return pd.DataFrame(data).to_json()

def clean_data(**kwargs):
    ti = kwargs['ti']
    df_json = ti.xcom_pull(task_ids='load_from_google_sheets')
    df = pd.read_json(df_json)
    df = df.dropna().drop_duplicates()
    ti.xcom_push(key='cleaned_data', value=df.to_json())

def scale_and_normalize(**kwargs):
    ti = kwargs['ti']
    df_json = ti.xcom_pull(task_ids='clean_data', key='cleaned_data')
    df = pd.read_json(df_json)
    numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
    scaler = StandardScaler()
    df[numeric_columns] = scaler.fit_transform(df[numeric_columns])
    normalizer = MinMaxScaler()
    df[numeric_columns] = normalizer.fit_transform(df[numeric_columns])
    ti.xcom_push(key='processed_data', value=df.to_json())

def save_processed_data(**kwargs):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
    client = gspread.authorize(creds)
    ti = kwargs['ti']
    processed_json = ti.xcom_pull(task_ids='scale_and_normalize', key='processed_data')
    df = pd.read_json(processed_json)
    processed_sheet = client.create('ProcessedDataset').sheet1
    processed_sheet.update([df.columns.values.tolist()] + df.values.tolist())

default_args = {'owner': 'airflow', 'start_date': datetime(2023, 10, 1)}

with DAG('dag2_process_data', default_args=default_args, schedule_interval=None, catchup=False) as dag2:
    load_task = PythonOperator(task_id='load_from_google_sheets', python_callable=load_from_google_sheets, provide_context=True)
    clean_task = PythonOperator(task_id='clean_data', python_callable=clean_data, provide_context=True)
    scale_task = PythonOperator(task_id='scale_and_normalize', python_callable=scale_and_normalize, provide_context=True)
    save_task = PythonOperator(task_id='save_processed_data', python_callable=save_processed_data, provide_context=True)
    load_task >> clean_task >> scale_task >> save_task
