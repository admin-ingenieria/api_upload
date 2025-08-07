import os
import pyodbc
from dotenv import load_dotenv
# Cargar variables de entorno
load_dotenv()

def get_db_connection():
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_DATABASE = os.getenv("DB_DATABASE")
    DB_PORT_STR = os.getenv("DB_PORT")
    DB_PORT = int(DB_PORT_STR)

    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_HOST};DATABASE={DB_DATABASE};UID={DB_USER};PWD={DB_PASSWORD}'
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        print("Conexión exitosa a SQL Server")
        return conn
    except Exception as e:
        print("Error de conexión:", e)
        return None