import os
from dotenv import load_dotenv
load_dotenv()

import requests
import mysql.connector
from datetime import datetime
from authtoken import obter_token
import time
from dateutil import parser
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
import atexit

def routeviolation(token):
    parana_tz = pytz.timezone("America/Sao_Paulo")
    hoje = datetime.now(parana_tz).date()

    url = "https://integration.systemsatx.com.br/GlobalBus/Trip/TripsWithNonConformity"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        conn = mysql.connector.connect(
            host=os.getenv("POWERBI_DB_HOST"),
            database=os.getenv("POWERBI_DB_NAME"),
            user=os.getenv("POWERBI_DB_USER"),
            password=os.getenv("POWERBI_DB_PASSWORD")
        )
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS informacoes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                LineName VARCHAR(255),
                RouteName VARCHAR(255),
                Direction VARCHAR(255),
                RealVehicle VARCHAR(255),
                url VARCHAR(512),
                data_execucao DATE,
                violation_type VARCHAR(255),
                UNIQUE (RouteName, data_execucao)
            )
        """)

        for coluna in ['url', 'violation_type']:
            try:
                cursor.execute(f"ALTER TABLE informacoes ADD COLUMN {coluna} VARCHAR(512)")
            except mysql.connector.Error as err:
                if err.errno != 1060:
                    raise

        initial_date = f"{hoje}T00:00:00.000Z"
        final_date = f"{hoje}T23:59:59.999Z"

        payload = {
            "ClientIntegrationCode": "1003",
            "InitialDate": initial_date,
            "FinalDate": final_date,
            "DelayTolerance": 5,
            "EarlinessTolerance": 5,
            "InconformityType": 1
        }

        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()

        print(f"📅 Processando violações de {hoje}...")

        if not data:
            print("🔸 Nenhuma violação encontrada.")
        else:
            if isinstance(data, dict):
                data = [data]

            insert_data = []

            for item in data:
                route_name = item.get("RouteName")
                if not route_name:
                    continue

                original_url = item.get("URL", "")
                url = original_url.replace("globalbus.com.br", "http://educacaorumocerto.trackland.com.br/") if original_url else None

                insert_data.append((
                    item.get("LineName"),
                    route_name,
                    item.get("Direction"),
                    item.get("RealVehicle"),
                    url,
                    hoje
                ))

            try:
                for record in insert_data:
                    cursor.execute("""
                        INSERT IGNORE INTO informacoes (
                            LineName, RouteName, Direction, RealVehicle, url, data_execucao
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                    """, record)
                conn.commit()
                print(f"✅ {cursor.rowcount} violações salvas para {hoje}.")
            except mysql.connector.Error as db_err:
                if db_err.errno == 1062:
                    print("⚠️ Algumas rotas já estavam salvas (duplicatas ignoradas).")
                else:
                    print("❌ Erro no banco de dados:", db_err)

        conn.close()

    except requests.exceptions.RequestException as e:
        print("❌ Erro na requisição:", e)

def refresh_mv():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("POWERBI_DB_HOST"),
            database=os.getenv("POWERBI_DB_NAME"),
            user=os.getenv("POWERBI_DB_USER"),
            password=os.getenv("POWERBI_DB_PASSWORD")
        )
        cursor = conn.cursor()

        print(f"🔄 Atualizando a Materialized View (MV) às {datetime.now()}...")

        cursor.execute("TRUNCATE TABLE informacoes_com_cliente_mv;")

        cursor.execute("""
            INSERT INTO informacoes_com_cliente_mv
            SELECT 
                i.id,
                i.LineName,
                i.RouteName,
                i.Direction,
                i.RealVehicle,
                i.data_execucao,
                i.url,
                i.violation_type,
                g.client_name,
                h.real_departure,
                h.real_arrival,
                h.id AS ID_GRADE
            FROM 
                u834686159_powerbi.informacoes i
            JOIN 
                u834686159_powerbi.graderumocerto g 
                ON TRIM(LCASE(i.RouteName)) = TRIM(LCASE(g.route_name))
            LEFT JOIN 
                u834686159_powerbi.historico_grades h
                ON TRIM(LCASE(i.RouteName)) = TRIM(LCASE(h.route_name))
                AND i.data_execucao = h.data_registro;
        """)
        conn.commit()
        print("✅ MV atualizada com sucesso.")
        conn.close()

    except Exception as e:
        print(f"❌ Erro ao atualizar a MV: {e}")

def verificar_violações_por_velocidade(token):
    def conectar_mysql():
        return mysql.connector.connect(
            host=os.getenv("POWERBI_DB_HOST"),
            database=os.getenv("POWERBI_DB_NAME"),
            user=os.getenv("POWERBI_DB_USER"),
            password=os.getenv("POWERBI_DB_PASSWORD")
        )

    parana_tz = pytz.timezone("America/Sao_Paulo")

    conn = conectar_mysql()
    cursor = conn.cursor(dictionary=True)

    batch_size = 100
    offset = 0
    while True:
        cursor.execute("""
            SELECT RealVehicle, real_departure, real_arrival, RouteName, violation_type
            FROM informacoes_com_cliente
            WHERE real_departure IS NOT NULL AND real_arrival IS NOT NULL
            LIMIT %s OFFSET %s
        """, (batch_size, offset))
        registros = cursor.fetchall()
        if not registros:
            break

        for reg in registros:
            if reg.get('violation_type'):
                print(f"⏩ Pulando {reg['RouteName']} ({reg['RealVehicle']}) — violação já registrada: {reg['violation_type']}")
                continue

            try:
                vehicle_code = reg['RealVehicle']
                start = reg['real_departure']
                end = reg['real_arrival']
                route_name = reg['RouteName']

                if not (vehicle_code and start and end):
                    continue

                start_dt = parser.parse(start, dayfirst=True) if isinstance(start, str) else start
                end_dt = parser.parse(end, dayfirst=True) if isinstance(end, str) else end

                start_dt = parana_tz.localize(start_dt).astimezone(pytz.utc)
                end_dt = parana_tz.localize(end_dt).astimezone(pytz.utc)

                payload = {
                    "TrackedUnitType": 1,
                    "TrackedUnitIntegrationCode": vehicle_code,
                    "StartDatePosition": start_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "EndDatePosition": end_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                }

                headers = {"Authorization": f"Bearer {token}"}

                response = requests.post(
                    "https://integration.systemsatx.com.br/Controlws/HistoryPosition/List",
                    json=payload,
                    headers=headers
                )

                if response.status_code == 204:
                    continue
                elif response.status_code == 200 and response.content:
                    try:
                        positions = response.json()
                    except Exception:
                        continue
                else:
                    continue

                violacao = "Desvio de Rota"
                for pos in positions:
                    if pos.get("Velocity", 0) > 70:
                        violacao = "Velocidade Excedida"
                        break

                data_execucao = start_dt.strftime('%Y-%m-%d')

                try:
                    conn.ping(reconnect=True)
                except Exception:
                    conn = conectar_mysql()
                    cursor = conn.cursor(dictionary=True)

                cursor.execute("""
                    SELECT id FROM informacoes
                    WHERE RouteName = %s AND data_execucao = %s
                    LIMIT 1
                """, (route_name, data_execucao))
                row = cursor.fetchone()

                if row:
                    cursor.execute("""
                        UPDATE informacoes
                        SET violation_type = %s
                        WHERE id = %s
                    """, (violacao, row['id']))
                    conn.commit()
                else:
                    print(f"❌ Linha não encontrada ➤ {route_name} ({data_execucao})")

                time.sleep(1)

            except Exception as e:
                print(f"💥 Erro inesperado na rota {reg.get('RouteName')} ({reg.get('RealVehicle')}): {e}")
                continue

        offset += batch_size

    conn.close()

def iniciar_agendador():
    scheduler = BackgroundScheduler()
    scheduler.add_job(refresh_mv, 'interval', hours=1, id='refresh_mv_job', next_run_time=datetime.now())
    scheduler.start()
    print("⏱️ Agendador de atualização da MV iniciado.")

    atexit.register(lambda: scheduler.shutdown(wait=True))

if __name__ == '__main__':
    token = obter_token()
    if token:
        iniciar_agendador()
        routeviolation(token)
        verificar_violações_por_velocidade(token)

        try:
            while True:
                time.sleep(60)
        except (KeyboardInterrupt, SystemExit):
            print("🛑 Encerrando o script...")
    else:
        print("❌ Não foi possível obter o token.")
