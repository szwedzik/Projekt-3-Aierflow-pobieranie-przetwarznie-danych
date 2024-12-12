import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from sklearn.model_selection import train_test_split
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Ścieżka do klucza Google Sheets
CREDENTIALS_PATH = "/opt/airflow/dags/credentials/google_sheets_key.json"
DATASET_URL = "https://vincentarelbundock.github.io/Rdatasets/csv/AER/CollegeDistance.csv"

def download_data(**kwargs):
    response = requests.get(DATASET_URL)
    response.raise_for_status()
    df = pd.read_csv(pd.compat.StringIO(response.text))
    return df.to_json()

def split_data(**kwargs):
    ti = kwargs['ti']
    df_json = ti.xcom_pull(task_ids='download_data')
    df = pd.read_json(df_json)
    train_df, test_df = train_test_split(df, test_size=0.3, random_state=42)
    ti.xcom_push(key='train_data', value=train_df.to_json())
    ti.xcom_push(key='test_data', value=test_df.to_json())

def save_to_google_sheets(**kwargs):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
    client = gspread.authorize(creds)
    ti = kwargs['ti']
    train_json = ti.xcom_pull(task_ids='split_data', key='train_data')
    test_json = ti.xcom_pull(task_ids='split_data', key='test_data')
    train_df = pd.read_json(train_json)
    test_df = pd.read_json(test_json)
    train_sheet = client.create('ModelDataset').sheet1
    test_sheet = client.create('RetrainDataset').sheet1
    train_sheet.update([train_df.columns.values.tolist()] + train_df.values.tolist())
    test_sheet.update([test_df.columns.values.tolist()] + test_df.values.tolist())

default_args = {'owner': 'airflow', 'start_date': datetime(2023, 10, 1)}

with DAG('dag1_download_and_split', default_args=default_args, schedule_interval=None, catchup=False) as dag1:
    download_task = PythonOperator(task_id='download_data', python_callable=download_data, provide_context=True)
    split_task = PythonOperator(task_id='split_data', python_callable=split_data, provide_context=True)
    save_task = PythonOperator(task_id='save_to_google_sheets', python_callable=save_to_google_sheets, provide_context=True)
    download_task >> split_task >> save_task
