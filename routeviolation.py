import os
from dotenv import load_dotenv
load_dotenv()

import requests
import mysql.connector
from datetime import datetime
from authtoken import obter_token
import pytz

def routeviolation():
    parana_tz = pytz.timezone("America/Sao_Paulo")
    hoje = datetime.now(parana_tz).date()

    token = obter_token()
    if not token:
        print("❌ Token inválido.")
        return

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

        # Criação da tabela (não altera se já existir)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS informacoes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                LineName VARCHAR(255),
                RouteName VARCHAR(255),
                Direction VARCHAR(255),
                RealVehicle VARCHAR(255),
                url VARCHAR(512),
                data_execucao DATE,
                UNIQUE (RouteName, data_execucao)
            )
        """)

        # Garante que a coluna 'url' existe
        try:
            cursor.execute("ALTER TABLE informacoes ADD COLUMN url VARCHAR(512)")
        except mysql.connector.Error as err:
            if err.errno != 1060:  # 1060 = Duplicate column name
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

if __name__ == '__main__':
    routeviolation()
