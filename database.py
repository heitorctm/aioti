import psycopg2
from psycopg2 import OperationalError, DatabaseError
import json
import time
from env import db_name, db_user, db_password, db_host,db_port


def conectar_ao_banco(max_retries=5, retry_delay=2):
    retries = 0
    while retries < max_retries:
        try:
            conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
            return conn
        except OperationalError as e:
            print(f"Falha na conexão ao banco de dados, tentativa {retries + 1}/{max_retries}: {e}")
            retries += 1
            time.sleep(retry_delay)
    print("Não foi possível conectar ao banco de dados após várias tentativas.")
    return None

def verificar_ou_inserir_strings(dados_str):
    conn = conectar_ao_banco()
    if not conn:
        return
    try:
        cur = conn.cursor()
        for dado_str in dados_str:
            dado_dict = json.loads(dado_str)
            cur.execute("INSERT INTO strings (id) VALUES (%s) ON CONFLICT (id) DO NOTHING;", (dado_dict['string_id'],))
        conn.commit()
    except DatabaseError as e:
        print("Erro ao inserir strings no banco:", e)
    finally:
        conn.close()

def inserir_no_banco(dados_json, tipo):
    conn = conectar_ao_banco()
    if not conn:
        return
    try:
        cur = conn.cursor()
        for dado_str in dados_json:
            dado_dict = json.loads(dado_str)
            colunas = ', '.join(dado_dict.keys())
            placeholders = ', '.join(['%s'] * len(dado_dict))
            tabela = 'entrada_inversores' if tipo == 'inversor' else 'entrada_strings'
            sql = f"INSERT INTO {tabela} ({colunas}) VALUES ({placeholders})"
            cur.execute(sql, tuple(dado_dict.values()))
        conn.commit()
    except DatabaseError as e:
        print(f"Erro ao inserir no banco na tabela {tabela}:", e)
    finally:
        conn.close()
