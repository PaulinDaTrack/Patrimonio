import os
import requests
import mysql.connector
from dotenv import load_dotenv
from authtoken import obter_token
from datetime import datetime, timedelta, date

load_dotenv()

def atualizar_odometro_para_veiculo(line_integration_code, real_departure_db, real_arrival_db, tracked_unit_integration_code, real_departure_api=None, real_arrival_api=None):
    token = obter_token()
    if not token:
        print('Token não obtido')
        return

    start_api = real_departure_api if real_departure_api else real_departure_db
    end_api = real_arrival_api if real_arrival_api else real_arrival_db

    api_url = "https://integration.systemsatx.com.br/Controlws/HistoryPosition/List"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "TrackedUnitType": 1,
        "TrackedUnitIntegrationCode": tracked_unit_integration_code,
        "StartDatePosition": start_api,
        "EndDatePosition": end_api
    }
    print('Payload enviado para a API:', payload)
    response = requests.post(api_url, headers=headers, json=payload)
    print('Status code da resposta:', response.status_code)
    if response.text.startswith('['):
        try:
            data_preview = response.json()
            print(f'Resposta da API: {len(data_preview)} registros, primeiro: {data_preview[0] if data_preview else "vazio"}')
        except Exception:
            print('Resposta da API: erro ao processar preview')
    else:
        print(f'Resposta da API: {response.text[:200]}...')
    if response.status_code != 200:
        print('Erro na API:', response.status_code)
        return
    data = response.json()
    odometro = None
    if isinstance(data, list) and data:
        for item in data:
            if 'Odometer' in item:
                odometro = item['Odometer']
    elif isinstance(data, dict) and 'Odometer' in data:
        odometro = data['Odometer']
    if odometro is None:
        print('Odômetro não encontrado na resposta')
        return
    try:
        conn = mysql.connector.connect(
            host=os.getenv("POWERBI_DB_HOST"),
            database=os.getenv("POWERBI_DB_NAME"),
            user=os.getenv("POWERBI_DB_USER"),
            password=os.getenv("POWERBI_DB_PASSWORD")
        )
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE historico_grades
            SET odometro = %s
            WHERE line = %s AND real_departure = %s AND real_arrival = %s
        """, (str(odometro), line_integration_code, real_departure_db, real_arrival_db))
        conn.commit()
        print(f"Odômetro atualizado para {odometro}")
        cursor.close()
        conn.close()
    except Exception as e:
        print('Erro ao atualizar banco:', e)

def get_mysql_conn():
    return mysql.connector.connect(
        host=os.getenv("POWERBI_DB_HOST"),
        database=os.getenv("POWERBI_DB_NAME"),
        user=os.getenv("POWERBI_DB_USER"),
        password=os.getenv("POWERBI_DB_PASSWORD")
    )

def get_estimated_distance(veiculo, line, real_departure, real_arrival):
    try:
        conn = get_mysql_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT estimated_distance FROM historico_grades
            WHERE real_vehicle = %s AND line = %s AND real_departure = %s AND real_arrival = %s
        """, (veiculo, line, real_departure, real_arrival))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row and row[0] is not None:
            try:
                return float(row[0])
            except Exception:
                return None
        return None
    except Exception as e:
        print('Erro ao buscar estimated_distance:', e)
        return None

def get_last_odometro(veiculo, real_departure):
    try:
        conn = get_mysql_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT odometro FROM historico_grades
            WHERE real_vehicle = %s AND real_arrival < %s AND odometro IS NOT NULL AND odometro != ''
            ORDER BY real_arrival DESC LIMIT 1
        """, (veiculo, real_departure))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row and row[0] is not None:
            try:
                return float(row[0])
            except Exception:
                return 0
        return 0
    except Exception as e:
        print('Erro ao buscar odômetro anterior:', e)
        return 0

def update_odometro(veiculo, line, real_departure, real_arrival, data_registro, odometro):
    try:
        conn = get_mysql_conn()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE historico_grades
            SET odometro = %s
            WHERE real_vehicle = %s AND line = %s AND real_departure = %s AND real_arrival = %s AND data_registro = %s
        """, (str(odometro), veiculo, line, real_departure, real_arrival, data_registro))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Odômetro atualizado para {odometro} na linha {line} para o veículo {veiculo}")
    except Exception as e:
        print('Erro ao atualizar banco:', e)

def main():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("POWERBI_DB_HOST"),
            database=os.getenv("POWERBI_DB_NAME"),
            user=os.getenv("POWERBI_DB_USER"),
            password=os.getenv("POWERBI_DB_PASSWORD")
        )
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT real_vehicle FROM historico_grades WHERE real_vehicle IS NOT NULL AND real_vehicle != '' AND (odometro IS NULL OR odometro = '' OR odometro = 'NULL')")
        veiculos = [row[0] for row in cursor.fetchall()]
        token = obter_token()
        if not token:
            print('Token não obtido')
            exit(1)
        mes = 9
        ano = 2025
        data_hoje = date.today()
        data_ontem = date.today() - timedelta(days=1)
        veiculos_linhas = []
        for veiculo in veiculos:
            try:
                conn_prev = get_mysql_conn()
                cursor_prev = conn_prev.cursor()
                cursor_prev.execute("""
                    SELECT line, real_departure, real_arrival, data_registro, odometro, estimated_distance
                    FROM historico_grades
                    WHERE real_vehicle = %s AND data_registro = %s AND real_departure IS NOT NULL AND real_arrival IS NOT NULL
                    ORDER BY real_arrival DESC LIMIT 1
                """, (veiculo, data_hoje))
                row_prev = cursor_prev.fetchone()
                if row_prev and (row_prev[4] is None or row_prev[4] == '' or row_prev[4] == 'NULL'):
                    odometro_ant = get_last_odometro(veiculo, row_prev[2])
                    try:
                        estimated_distance_prev = float(row_prev[5]) if row_prev[5] is not None else 0
                    except Exception:
                        estimated_distance_prev = 0
                    odometro_prev = odometro_ant + estimated_distance_prev
                    update_odometro(veiculo, row_prev[0], row_prev[1], row_prev[2], odometro_prev)
                cursor_prev.close()
                conn_prev.close()
            except Exception as e:
                print('Erro ao preencher odômetro da volta do dia anterior:', e)
            linhas_ord = []
            try:
                conn_ord = get_mysql_conn()
                cursor_ord = conn_ord.cursor()
                cursor_ord.execute("""
                    SELECT line, real_departure, real_arrival, data_registro
                    FROM historico_grades
                    WHERE real_vehicle = %s AND real_departure IS NOT NULL AND real_arrival IS NOT NULL AND data_registro = %s AND (odometro IS NULL OR odometro = '' OR odometro = 'NULL')
                    ORDER BY real_departure ASC
                """, (veiculo, data_hoje))
                linhas_ord = cursor_ord.fetchall()
                cursor_ord.close()
                conn_ord.close()
            except Exception as e:
                print('Erro ao buscar linhas ordenadas:', e)
            for line, real_departure, real_arrival, data_registro in linhas_ord:
                try:
                    dt_dep = datetime.strptime(real_departure, "%d/%m/%Y %H:%M:%S") + timedelta(hours=3)
                    dt_arr = datetime.strptime(real_arrival, "%d/%m/%Y %H:%M:%S") + timedelta(hours=3)
                    start_iso = dt_dep.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    end_iso = dt_arr.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                except Exception as e:
                    print(f"Erro ao converter datas para linha {line}: {e}")
                    continue
                estimated_distance = get_estimated_distance(veiculo, line, real_departure, real_arrival)
                payload = {
                    "TrackedUnitType": 1,
                    "TrackedUnitIntegrationCode": veiculo,
                    "StartDatePosition": start_iso,
                    "EndDatePosition": end_iso
                }
                print(f"Consultando odômetro para {veiculo} linha {line} no intervalo {start_iso} - {end_iso}")
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                }
                response = requests.post(
                    "https://integration.systemsatx.com.br/Controlws/HistoryPosition/List",
                    headers=headers, json=payload
                )
                if response.status_code == 401:
                    print('Token expirado, obtendo novo token...')
                    token = obter_token()
                    if not token:
                        print('Token não obtido')
                        continue
                    headers["Authorization"] = f"Bearer {token}"
                    response = requests.post(
                        "https://integration.systemsatx.com.br/Controlws/HistoryPosition/List",
                        headers=headers, json=payload
                    )
                print('Status code da resposta:', response.status_code)
                if response.text.startswith('['):
                    try:
                        data_preview = response.json()
                        print(f'Resposta da API: {len(data_preview)} registros, primeiro: {data_preview[0] if data_preview else "vazio"}')
                    except Exception:
                        print('Resposta da API: erro ao processar preview')
                else:
                    print(f'Resposta da API: {response.text[:200]}...')
                if response.status_code != 200:
                    print('Erro na API:', response.status_code)
                    continue
                data_api = response.json()
                odometro = None
                if isinstance(data_api, list) and data_api:
                    data_api_sorted = sorted([item for item in data_api if 'Odometer' in item and 'EventDate' in item], key=lambda x: x['EventDate'])
                    if len(data_api_sorted) >= 2:
                        odometro_ini = data_api_sorted[0]['Odometer']
                        odometro_fim = data_api_sorted[-1]['Odometer']
                        diff = abs(odometro_fim - odometro_ini)
                        modo = 'real'
                    elif len(data_api_sorted) == 1:
                        odometro_ini = odometro_fim = data_api_sorted[0]['Odometer']
                        diff = None
                        modo = 'aproximado'
                    else:
                        print(f'Nenhum log de odômetro válido para {veiculo} linha {line}')
                        continue
                    odometro_ant = get_last_odometro(veiculo, real_departure)
                    if modo == 'real' and diff is not None:
                        odometro = abs(odometro_ant + diff)
                    elif modo == 'aproximado' and estimated_distance is not None:
                        odometro = abs(odometro_ant + estimated_distance)
                    else:
                        odometro = abs(odometro_ant)
                elif isinstance(data_api, dict) and 'Odometer' in data_api:
                    odometro = abs(data_api['Odometer'])
                if odometro is None:
                    print(f'Odômetro não encontrado para {veiculo} linha {line} no intervalo {start_iso} - {end_iso}')
                    continue
                update_odometro(veiculo, line, real_departure, real_arrival, data_registro, odometro)
    except Exception as e:
        print('Erro ao buscar veículos:', e)

if __name__ == "__main__":
    main()