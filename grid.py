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

    today_date = datetime.datetime.now(pytz.timezone("America/Sao_Paulo")).strftime("%d/%m/%Y")
    effective_date_iso = to_iso(today_date)
    payload = [
        {
            "PropertyName": "EffectiveDate",
            "Condition": "Equal",
            "Value": effective_date_iso
        }
    ]

    response_api = requests.post(api_url, headers=headers, json=payload)
    if response_api.status_code == 200:
        data = response_api.json()

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

        create_table_query = """
        CREATE TABLE IF NOT EXISTS graderumocerto (
            line VARCHAR(50),
            estimated_departure VARCHAR(50),
            estimated_arrival VARCHAR(50),
            real_departure VARCHAR(50),
            real_arrival VARCHAR(50),
            route_integration_code VARCHAR(255) NOT NULL,
            route_name VARCHAR(255),
            direction_name VARCHAR(255),
            shift VARCHAR(50),
            estimated_vehicle VARCHAR(255),
            real_vehicle VARCHAR(255),
            estimated_distance VARCHAR(50),
            travelled_distance VARCHAR(50),
            client_name VARCHAR(255),
            PRIMARY KEY (route_integration_code)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        create_historico_query = """
        CREATE TABLE IF NOT EXISTS historico_grades (
            id INT AUTO_INCREMENT PRIMARY KEY,
            line VARCHAR(50),
            estimated_departure VARCHAR(50),
            estimated_arrival VARCHAR(50),
            real_departure VARCHAR(50),
            real_arrival VARCHAR(50),
            route_integration_code VARCHAR(255),
            route_name VARCHAR(255),
            direction_name VARCHAR(255),
            shift VARCHAR(50),
            estimated_vehicle VARCHAR(255),
            real_vehicle VARCHAR(255),
            estimated_distance VARCHAR(50),
            travelled_distance VARCHAR(50),
            client_name VARCHAR(255),
            data_registro DATE,
            UNIQUE KEY idx_codigo_data (route_integration_code, data_registro)
        );
        """
        cursor.execute(create_historico_query)
        conn.commit()

        update_query = """
        UPDATE graderumocerto
        SET estimated_departure = %s,
            estimated_arrival = %s,
            real_departure = %s,
            real_arrival = %s,
            real_vehicle = %s,
            estimated_distance = %s,
            travelled_distance = %s
        WHERE route_integration_code = %s
        """

        insert_query = """
        INSERT INTO graderumocerto (
            line, estimated_departure, estimated_arrival, real_departure, real_arrival, 
            route_name, direction_name, shift, estimated_vehicle, real_vehicle, 
            estimated_distance, travelled_distance, client_name, route_integration_code
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        insert_historico_query = '''
        INSERT INTO historico_grades (
            line, estimated_departure, estimated_arrival, real_departure, real_arrival,
            route_integration_code, route_name, direction_name, shift,
            estimated_vehicle, real_vehicle, estimated_distance, travelled_distance, client_name, data_registro
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURDATE())
        ON DUPLICATE KEY UPDATE
            estimated_departure = VALUES(estimated_departure),
            estimated_arrival = VALUES(estimated_arrival),
            real_departure = VALUES(real_departure),
            real_arrival = VALUES(real_arrival),
            real_vehicle = VALUES(real_vehicle),
            estimated_vehicle = VALUES(estimated_vehicle),
            estimated_distance = VALUES(estimated_distance),
            travelled_distance = VALUES(travelled_distance),
            route_name = VALUES(route_name),
            direction_name = VALUES(direction_name),
            shift = VALUES(shift),
            client_name = IFNULL(VALUES(client_name), client_name),
            line = VALUES(line)
        '''

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
            client_name_result = cursor.fetchone()
            client_name = item.get('ClientName') or (client_name_result[0] if client_name_result else None)

            cursor.execute(insert_historico_query, (
                line, estimated_departure, estimated_arrival, real_departure, real_arrival,
                route_integration_code, route_name, direction_name, shift,
                estimated_vehicle, real_vehicle, estimated_distance, travelled_distance, client_name
            ))

            cursor.execute("SELECT route_integration_code FROM graderumocerto WHERE route_integration_code = %s", (route_integration_code,))
            result = cursor.fetchone()
            if result:
                cursor.execute(update_query, (
                    estimated_departure,
                    estimated_arrival,
                    real_departure,
                    real_arrival,
                    real_vehicle,
                    estimated_distance,
                    travelled_distance,
                    route_integration_code
                ))
            else:
                dados = (line, estimated_departure, estimated_arrival, real_departure, real_arrival,
                         route_name, direction_name, shift, estimated_vehicle, real_vehicle,
                         estimated_distance, travelled_distance, client_name, route_integration_code)
                cursor.execute(insert_query, dados)

        conn.commit()
        cursor.close()
        conn.close()
    else:
        print("Erro na requisição da API:", response_api.status_code, response_api.text)

if __name__ == '__main__':
    processar_grid()
