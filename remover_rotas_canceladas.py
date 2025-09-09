import os
from dotenv import load_dotenv
load_dotenv()

import requests
import datetime
import mysql.connector
import pytz
from authtoken import obter_token

def remover_rotas_canceladas(dias_verificar=10):
    token = obter_token()
    if not token:
        print("Não foi possível obter token.")
        return

    api_url = "https://integration.systemsatx.com.br/GlobalBus/Grid/List?paramClientIntegrationCode=1003"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

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

    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT route_integration_code FROM graderumocerto")
    routes_in_db = {row[0] for row in cursor.fetchall() if row[0]}

    canceled_map = {}
    missing_map = {}
    now = datetime.datetime.now(pytz.timezone("America/Sao_Paulo"))
    for i in range(dias_verificar):
        data_alvo = now - datetime.timedelta(days=i)
        data_iso = data_alvo.strftime("%Y-%m-%dT00:00:00Z")
        payload = [{"PropertyName": "EffectiveDate", "Condition": "Equal", "Value": data_iso}]
        try:
            resp = requests.post(api_url, headers=headers, json=payload, timeout=30)
        except Exception as e:
            print(f"Erro ao consultar API para {data_alvo.date()}: {e}")
            continue

        if resp.status_code != 200:
            print(f"API retornou {resp.status_code} para {data_alvo.date()}")
            continue

        try:
            items = resp.json()
        except Exception:
            continue

        api_present = {it.get('RouteIntegrationCode') for it in items if it.get('RouteIntegrationCode')}

        for item in items:
            if item.get('IsTripCanceled') is True:
                code = item.get('RouteIntegrationCode')
                if code and code in routes_in_db:
                    canceled_map.setdefault(code, set()).add(data_alvo.date())

        cursor.execute("SELECT DISTINCT route_integration_code FROM historico_grades WHERE data_registro = %s", (data_alvo.date(),))
        db_codes_date = {row[0] for row in cursor.fetchall() if row[0]}

        to_remove = db_codes_date - api_present
        for code in to_remove:
            if code in routes_in_db:
                missing_map.setdefault(code, set()).add(data_alvo.date())

    if not canceled_map and not missing_map:
        print("Nenhuma rota cancelada ou ocorrência ausente encontrada no período verificado.")
        cursor.close()
        conn.close()
        return

    if canceled_map:
        print("Removendo ocorrências marcadas como canceladas (historico_grades):")
        for code, dates in canceled_map.items():
            for dt in dates:
                try:
                    cursor.execute(
                        "DELETE FROM historico_grades WHERE route_integration_code = %s AND data_registro = %s",
                        (code, dt)
                    )
                    print(f"  - {code} removida do historico em {dt} (cancelada)")
                except Exception as e:
                    print(f"Erro removendo historico {code} em {dt}: {e}")
                    conn.rollback()

    if missing_map:
        print("Removendo ocorrências ausentes na API (historico_grades):")
        for code, dates in missing_map.items():
            for dt in dates:
                try:
                    cursor.execute(
                        "DELETE FROM historico_grades WHERE route_integration_code = %s AND data_registro = %s",
                        (code, dt)
                    )
                    print(f"  - {code} removida do historico em {dt} (ausente na API)")
                except Exception as e:
                    print(f"Erro removendo historico ausente {code} em {dt}: {e}")
                    conn.rollback()

    conn.commit()
    cursor.close()
    conn.close()
    print("Remoção concluída.")

if __name__ == "__main__":
	import sys
	try:
		if len(sys.argv) > 1:
			dias = int(sys.argv[1])
		elif os.getenv("POWERBI_DIAS_VERIFICAR"):
			dias = int(os.getenv("POWERBI_DIAS_VERIFICAR"))
		else:
			dias = 10
	except Exception:
		dias = 10

	remover_rotas_canceladas(dias_verificar=dias)
