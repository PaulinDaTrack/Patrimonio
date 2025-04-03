__all__ = ['atualizar_ultima_execucao']

import os
from dotenv import load_dotenv
load_dotenv()

import mysql.connector
from mysql.connector import pooling  # Adicionado para pool de conexões
from datetime import datetime
import pytz  # novo para conversão de fuso

# Debug: confirmar as variáveis do banco
print("POWERBI_DB_HOST:", os.getenv("POWERBI_DB_HOST"))
print("POWERBI_DB_USER:", os.getenv("POWERBI_DB_USER"))
print("POWERBI_DB_PASSWORD:", os.getenv("POWERBI_DB_PASSWORD"))
print("POWERBI_DB_NAME:", os.getenv("POWERBI_DB_NAME"))

# Criar pool de conexões
db_config = {
    "host": os.getenv("POWERBI_DB_HOST"),
    "database": os.getenv("POWERBI_DB_NAME"),
    "user": os.getenv("POWERBI_DB_USER"),
    "password": os.getenv("POWERBI_DB_PASSWORD"),
}
connection_pool = pooling.MySQLConnectionPool(pool_name="mypool", pool_size=5, **db_config)

def atualizar_ultima_execucao():
    try:
        conn = connection_pool.get_connection()  # Obter conexão do pool
        cursor = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS ultima_execucao (
            id INT PRIMARY KEY,
            last_execution DATETIME
        );
        """
        cursor.execute(create_table_query)
        
        upsert_query = """
        INSERT INTO ultima_execucao (id, last_execution)
        VALUES (1, %s)
        ON DUPLICATE KEY UPDATE last_execution = VALUES(last_execution)
        """
        parana_tz = pytz.timezone("America/Sao_Paulo")  # Alterado para timezone reconhecido
        current_time = datetime.now(parana_tz)  # usar horário de São Paulo
        cursor.execute(upsert_query, (current_time,))
        
        conn.commit()
    except mysql.connector.Error as err:
        print("Erro ao conectar no banco de dados:", err)
    finally:
        cursor.close()
        conn.close()  # Retornar conexão ao pool
    print("Última execução atualizada para:", current_time)

if __name__ == '__main__':
    atualizar_ultima_execucao()