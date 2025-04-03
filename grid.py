import os
from dotenv import load_dotenv
load_dotenv()

import requests
import datetime
import mysql.connector
import pytz
from authtoken import obter_token

def format_date(date_str):
    if not date_str:
        return None
    try:
        dt = datetime.datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return date_str

def to_iso(date_str):
    try:
        dt = datetime.datetime.strptime(date_str, "%d/%m/%Y")
        return dt.strftime("%Y-%m-%dT00:00:00Z")
    except Exception:
        return date_str

def nullify_date(date_str):
    if date_str in ["01/01/1 00:00:00", "01/01/0001 00:00:00"]:
         return None
    return date_str

def processar_grid():
    token = obter_token()
    if not token:
        return

    api_url = "https://integration.systemsatx.com.br/GlobalBus/Grid/List?paramClientIntegrationCode=1003"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    try:
        conn = mysql.connector.connect(
            host=os.getenv("POWERBI_DB_HOST"),
            database=os.getenv("POWERBI_DB_NAME"),
            user=os.getenv("POWERBI_DB_USER"),
            password=os.getenv("POWERBI_DB_PASSWORD")
        )
    except mysql.connector.Error as err:
        print("Erro ao conectar no banco de dados:", err)
        return

    cursor = conn.cursor(buffered=True)

    dias_a_verificar = 3
    for i in range(dias_a_verificar):
        data_alvo = datetime.datetime.now(pytz.timezone("America/Sao_Paulo")) - datetime.timedelta(days=i)
        data_formatada = data_alvo.strftime("%d/%m/%Y")
        data_iso = to_iso(data_formatada)

        payload = [{"PropertyName": "EffectiveDate", "Condition": "Equal", "Value": data_iso}]
        response_api = requests.post(api_url, headers=headers, json=payload)

        if response_api.status_code != 200:
            print(f"Erro na requisição da API para {data_formatada}: {response_api.status_code}")
            continue

        data = response_api.json()
        if not data:
            print(f"Sem dados para {data_formatada}")
            continue

        for item in data:
            line = item.get('LineIntegrationCode')
            estimated_departure = nullify_date(format_date(item.get('EstimatedDepartureDate')))
            estimated_arrival = nullify_date(format_date(item.get('EstimatedArrivalDate')))
            real_departure = nullify_date(format_date(item.get('RealDepartureDate')))
            real_arrival = nullify_date(format_date(item.get('RealdArrivalDate')))
            route_integration_code = item.get('RouteIntegrationCode')
            route_name = item.get('RouteName')
            direction_name = item.get('DirectionName')
            shift = item.get('Shift')
            estimated_vehicle = item.get('EstimatedVehicle')
            real_vehicle = item.get('RealVehicle')
            estimated_distance = item.get('EstimatedDistance')
            travelled_distance = item.get('TravelledDistance')

            cursor.execute("SELECT client_name FROM graderumocerto WHERE route_integration_code = %s", (route_integration_code,))
            client_result = cursor.fetchone()
            client_name = item.get('ClientName') or (client_result[0] if client_result else None)

            # Historico
            cursor.execute("""
                SELECT id FROM historico_grades
                WHERE route_integration_code = %s AND data_registro = %s
            """, (route_integration_code, data_alvo.date()))
            existe = cursor.fetchone()

            if existe:
                cursor.execute("""
                    UPDATE historico_grades SET
                        line = %s,
                        estimated_departure = %s,
                        estimated_arrival = %s,
                        real_departure = %s,
                        real_arrival = %s,
                        route_name = %s,
                        direction_name = %s,
                        shift = %s,
                        estimated_vehicle = %s,
                        real_vehicle = %s,
                        estimated_distance = %s,
                        travelled_distance = %s,
                        client_name = IFNULL(%s, client_name)
                    WHERE route_integration_code = %s AND data_registro = %s
                """, (
                    line, estimated_departure, estimated_arrival, real_departure, real_arrival,
                    route_name, direction_name, shift, estimated_vehicle, real_vehicle,
                    estimated_distance, travelled_distance, client_name,
                    route_integration_code, data_alvo.date()
                ))
            else:
                cursor.execute("""
                    INSERT INTO historico_grades (
                        line, estimated_departure, estimated_arrival, real_departure, real_arrival,
                        route_integration_code, route_name, direction_name, shift,
                        estimated_vehicle, real_vehicle, estimated_distance, travelled_distance,
                        client_name, data_registro
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    line, estimated_departure, estimated_arrival, real_departure, real_arrival,
                    route_integration_code, route_name, direction_name, shift,
                    estimated_vehicle, real_vehicle, estimated_distance, travelled_distance,
                    client_name, data_alvo.date()
                ))

            # Tabela principal
            cursor.execute("SELECT route_integration_code FROM graderumocerto WHERE route_integration_code = %s", (route_integration_code,))
            if cursor.fetchone():
                cursor.execute("""
                    UPDATE graderumocerto SET
                        estimated_departure = %s,
                        estimated_arrival = %s,
                        real_departure = %s,
                        real_arrival = %s,
                        real_vehicle = %s,
                        estimated_distance = %s,
                        travelled_distance = %s
                    WHERE route_integration_code = %s
                """, (
                    estimated_departure, estimated_arrival, real_departure,
                    real_arrival, real_vehicle, estimated_distance, travelled_distance,
                    route_integration_code
                ))
            else:
                cursor.execute("""
                    INSERT INTO graderumocerto (
                        line, estimated_departure, estimated_arrival, real_departure, real_arrival, 
                        route_name, direction_name, shift, estimated_vehicle, real_vehicle, 
                        estimated_distance, travelled_distance, client_name, route_integration_code
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    line, estimated_departure, estimated_arrival, real_departure, real_arrival,
                    route_name, direction_name, shift, estimated_vehicle, real_vehicle,
                    estimated_distance, travelled_distance, client_name, route_integration_code
                ))

        conn.commit()
        print(f"✅ Grades processadas para {data_formatada}")

    cursor.close()
    conn.close()
