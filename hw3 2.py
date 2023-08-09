import sqlite3 as sql
from zipfile import ZipFile
import pandas as pd
import json
import requests
from bs4 import BeautifulSoup as bs
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import re

def egrul(**kwargs):
    try:
        bd = SqliteHook(sqlite_conn_id='hw3')
        connection = bd.get_conn()
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE egrul (ogrn text, inn text, kpp text, name text, КодОКВЭД text);")
        connection.commit()
        with ZipFile("/home/vitaliy/egrul.json.zip", "r") as zf:
            file_names = zf.namelist()
            for name in file_names:
                data = json.loads(zf.read(name).decode())
                extracted = pd.json_normalize(data)
                #делаем проверку на наличие столбцов
                try:
                    extracted = extracted[['ogrn','inn','kpp','name','data.СвОКВЭД.СвОКВЭДОсн.КодОКВЭД']]
                except KeyError:
                    continue
                extracted.rename(columns = {'data.СвОКВЭД.СвОКВЭДОсн.КодОКВЭД':'КодОКВЭД'}, inplace = True)
                extracted = extracted.dropna()
                #создаем отфильтрованную таблицу для загрузки в бд
                egrul = extracted[extracted.КодОКВЭД.str.startswith('61')]
                egrul.to_sql('egrul', conn, if_exists='append', index = bool)
                connection.commit()
    except sql.Error as error:
        print(f"Не удалось вставить данные в таблицу")
        print("Исключение: ", error.__class__, error.args)

def vacancies(**kwargs):
    url = "https://api.hh.ru/vacancies?text=middle+python+developer&area=113&per_page=100&search_field=name" 
    headers = {'User-agent': 'Mozilla/5.0'}
    result = requests.get(url)
    if result.status_code == 200:
        vacancies = result.json().get('items')
        df = []
        for i, vacancy in enumerate(vacancies):
            url = vacancy['url']
            result = requests.get(url)
            vacancy = result.json()
            key = []
            description = ''
            if vacancy['key_skills'] != None:
                key_skills = vacancy['key_skills']
                if key_skills:
                    for skill in key_skills:
                    key.append(skill['name'])
                else:
                    key.append('нет')
            key = ', '.join(key)
            if vacancy['description'] != None:
                description = vacancy['description']   
            dict_ = {'company': vacancy['employer']['name'], 'name': vacancy['name'],
                    'description': description, 'key_skills': key}
            df.append(dict_)
        df = pd.DataFrame(df)
    else:
        print(f'Не удалось подключиться: {search_result.status_code}')
    
    try:
        conn = SqliteHook(sqlite_conn_id='hw3')
        cur = conn.get_conn()
        cur.execute("CREATE TABLE API (company text, name text, description text, key_skills text);")
        df.to_sql('API', conn, if_exists='append', index = bool)
        conn.commit()
        print('Таблица загружена')
    except sql.Error as error:
        print(f"Не удалось вставить данные в таблицу")
        print("Исключение: ", error.__class__, error.args)    
        
    conn.close()
    print("Соединение закрыто")

def skills(**kwargs):
        conn = SqliteHook(sqlite_conn_id='hw3')
        cur = conn.get_conn()
        cursor = cur.cursor()
        select = """"select distinct egrul.name, egrul.kpp from egrul"""
        cursor.execute(select)
        egrul = {}
        r = []
        for i, kpp in cur.fetchall():
            line = re.sub('["",() -'']', '', str(i))
            line = re.sub('[OOO]', '', line)
            line = re.sub('[ЗАО]', '', line)
            egrul[line] = kpp
        select2 = """select distinct API.company, API.key_skills from API"""    
        cursor.execute(select2)
        API = {}
        for name, skills in cur.fetchall():
            line = re.sub('["",() -'']', '', str(name))
            line = re.sub('[OOO]', '', line)
            line = re.sub('[ЗАО]', '', line)
            skills = skills.split(', ')
            API[line] = skills
        w = []
        a = {}
        for i in egrul:
            for g, skills in API.items():
                if i == g:
                    w+=skills
        for i in w:
            if a.keys != i:
                a[i] = w.count(i)

        f = [n for n, _ in sorted(a.items(), key=lambda x: x[1], reverse=True)][:10]
        s = " ".join(f)
        with open("skills.txt", "w") as file:
            file.write(s)

egrul_web = 'https://ofdata.ru/open-data/download/egrul.json.zip'
egrul_local = '/home/vitaliy/egrul.json.zip'

default_args = {
    'owner': 'vitaliy',
    'retries': 1,
    'retry_delay': timedelta(seconds=10)}

with DAG(
    dag_id='hw3',
    default_args=default_args,
    description='DAG for hw3',
    start_date=datetime(2023, 8, 10),
    schedule='@daily'
) as dag:
    download_egrul = BashOperator(
        task_id='download_egrul',
        bash_command=f"wget {egrul_web} -O {egrul_local}"
    )
    load_telecoms = PythonOperator(
        task_id='egrul',
        python_callable=egrul,
    )
    get_vacancies = PythonOperator(
        task_id='vacancies',
        python_callable=vacancies,
    )
    top_skills = PythonOperator(
        task_id='top_skills',
        python_callable=skills,
    )
    download_egrul >> egrul >> vacancies >> top_skills