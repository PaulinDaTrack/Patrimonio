import os
from dotenv import load_dotenv
load_dotenv()

import requests
import json
import mysql.connector
from datetime import datetime  # Removido timezone e timedelta
from authtoken import obter_token
import pytz  # novo para conversão de fuso

def routeviolation():
    parana_tz = pytz.timezone("America/Sao_Paulo")  # Alterado para timezone reconhecido
    current_time = datetime.now(parana_tz)  # horário em São Paulo
    hoje = current_time.date().isoformat()
    initial_date = f"{hoje}T00:00:00.000Z"
    final_date   = f"{hoje}T23:59:59.999Z"
    
    payload = {
        "ClientIntegrationCode": "1003",
        "InitialDate": initial_date,
        "FinalDate": final_date,
        "DelayTolerance": 5,
        "EarlinessTolerance": 5,
        "InconformityType": 1
    }
    
    url = "https://integration.systemsatx.com.br/GlobalBus/Trip/TripsWithNonConformity"
    token = obter_token()
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        # Filtra apenas os campos desejados
        if isinstance(data, list):
            filtered = [
                {
                    "LineName": item.get("LineName"),
                    "RouteName": item.get("RouteName"),
                    "Direction": item.get("Direction"),
                    "RealVehicle": item.get("RealVehicle")
                }
                for item in data
            ]
        else:
            filtered = {
                "LineName": data.get("LineName"),
                "RouteName": data.get("RouteName"),
                "Direction": data.get("Direction"),
                "RealVehicle": data.get("RealVehicle")
            }
        print(json.dumps(filtered, indent=4, ensure_ascii=False))
        
        # Conectar ao banco MySQL e atualizar as informações
        conn = mysql.connector.connect(
            host=os.getenv("POWERBI_DB_HOST"),
            database=os.getenv("POWERBI_DB_NAME"),
            user=os.getenv("POWERBI_DB_USER"),
            password=os.getenv("POWERBI_DB_PASSWORD")
        )
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS informacoes (
                LineName VARCHAR(255),
                RouteName VARCHAR(255) PRIMARY KEY,
                Direction VARCHAR(255),
                RealVehicle VARCHAR(255),
                data_execucao DATE
            )
        """)
        # Apagar registros que não sejam de hoje
        cursor.execute("DELETE FROM informacoes WHERE data_execucao <> %s", (hoje,))
        
        if isinstance(filtered, list):
            for item in filtered:
                cursor.execute("""
                    INSERT IGNORE INTO informacoes (LineName, RouteName, Direction, RealVehicle, data_execucao)
                    VALUES (%s, %s, %s, %s, %s)
                """, (item["LineName"], item["RouteName"], item["Direction"], item["RealVehicle"], hoje))
        else:
            cursor.execute("""
                INSERT IGNORE INTO informacoes (LineName, RouteName, Direction, RealVehicle, data_execucao)
                VALUES (%s, %s, %s, %s, %s)
            """, (filtered["LineName"], filtered["RouteName"], filtered["Direction"], filtered["RealVehicle"], hoje))
        conn.commit()
        conn.close()
    except requests.exceptions.RequestException as e:
        print("Erro na requisição:", e)
