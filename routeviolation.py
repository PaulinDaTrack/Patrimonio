import os
from dotenv import load_dotenv
load_dotenv()

import requests
import json
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

        # Cria tabela com id como chave primária
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS informacoes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                LineName VARCHAR(255),
                RouteName VARCHAR(255),
                Direction VARCHAR(255),
                RealVehicle VARCHAR(255),
                data_execucao DATE
            )
        """)

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
                insert_data.append((
                    item.get("LineName"),
                    item.get("RouteName"),
                    item.get("Direction"),
                    item.get("RealVehicle"),
                    hoje
                ))

            cursor.executemany("""
                INSERT INTO informacoes (
                    LineName, RouteName, Direction, RealVehicle, data_execucao
                ) VALUES (%s, %s, %s, %s, %s)
            """, insert_data)
            conn.commit()

            print(f"✅ {len(insert_data)} violações salvas para {hoje}.")

        conn.close()

    except requests.exceptions.RequestException as e:
        print("❌ Erro na requisição:", e)

    except mysql.connector.Error as db_err:
        print("❌ Erro no banco de dados:", db_err)
