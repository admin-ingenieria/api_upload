from fastapi import FastAPI, Request, File, UploadFile, Form
import pandas as pd
from typing import List, Optional
from fastapi import FastAPI, Query
from dotenv import load_dotenv
from datetime import date, time,datetime
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO
from difflib import SequenceMatcher
import traceback
import os
import pyodbc
import re

# Cargar variables de entorno
load_dotenv()

# Configurar FastAPI
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

@app.get('/')
def prueba():
    return "API corriendo"

@app.post("/personal_excel")
async def personal_excel(
    file: UploadFile = File(...),
    fecha_carga: date = Form(...),
    hora_carga: time = Form(...),
    flujo: str = Form(...),
    ruc: str = Form(...)
):
    contents = await file.read()
    df = pd.read_excel(BytesIO(contents))
    # Verificación de columnas vacías
    empty_data = {}
    for column in df.columns:
        empty_rows = df[df[column].isnull()].index.tolist()
        if empty_rows:
            empty_data[column] = [i + 2 for i in empty_rows]

    if empty_data:
        return {
            "status": 0,
            "message": "El archivo contiene campos vacíos en las siguientes columnas:",
            "empty_columns": list(empty_data.keys()),
            "empty_cells": empty_data
        }
    session = get_db_connection()
    errores = []
    try:
        cursor = session.cursor()
        # Eliminar anterior de flujo
        cursor.execute("DELETE FROM apl_imperio.APP_SALESFORCE_personal WHERE Flujo = ? AND RUC = ? ",(flujo,ruc))
        # Insertar cada fila del DataFrame
        for index, row in df.iterrows():
            try:
                print("Ingreso aqui")
                valores = (
                    row['PICKUP'], row['TIPO'], row['PLACA'], row['NOMBRES'], row['DOCUMENTO'],row['CARGO'],
                    row['EMPRESA'], row['RUC'],fecha_carga, hora_carga, flujo
                )
                cursor.execute("""
                    INSERT INTO apl_imperio.APP_SALESFORCE_Personal
                    (PICKUP,TIPO,placa, nombre, documento,cargo,empresa,RUC, fecha_carga, hora_carga, flujo)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,?,?)
                """, valores)
            except Exception as e:
                mensaje_error = str(e)
                columna_error, valor_error = identificar_columna_y_valor_error(mensaje_error, row)
                errores.append({
                    "fila": index + 2,
                    "detalle": mensaje_error,
                    "fila_contenido": row.to_dict(),
                    "columna_problematica": columna_error,
                    "valor_problematico": valor_error
                })
                print("❌ Error en fila", index + 2, ":", mensaje_error)
                raise
        session.commit()
        return {
            "status": 1,
            "message": "Todos los registros fueron insertados correctamente."
        }
    except Exception as e:
        session.rollback()
        print("❌ Error general:", str(e))
        print("📛 Traceback:")
        print(traceback.format_exc())
        return {
            "status": 0,
            "message": "❌ Ocurrió un error y no se insertó nada.",
            "errores": errores,
            "detalle": str(e),
            "trace": traceback.format_exc()
        }

    finally:
        session.close()
        
@app.post("/upload_excel")
async def upload_excel(
    file: UploadFile = File(...),
    fecha_carga: date = Form(...),
    hora_carga: time = Form(...),
    nombre_flujo: str = Form(...)
):
    contents = await file.read()
    df = pd.read_excel(BytesIO(contents))

    # Limpieza de columna Cita
    col_cita = encontrar_columna_similar(df, "Cita", 0.75)
    if col_cita:
        print(f"Columna detectada similar a 'Cita': '{col_cita}'")
        # Renombrar la columna a "Cita" para trabajar con ella más fácil
        df.rename(columns={col_cita: "Cita"}, inplace=True)
    else:
        print("No se encontró una columna similar a 'Cita'")
    
    
    if 'Cita' in df.columns:
        df['Cita'] = df['Cita'].apply(lambda x: "" if pd.isna(x) or str(x).strip() in ["", "-", " ", "'-"] else x)
        df['Cita'] = df['Cita'].fillna('')
    else:
        pass
    
    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce', dayfirst=True).dt.date

    # Verificación de columnas vacías
    empty_data = {}
    for column in df.columns:
        empty_rows = df[df[column].isnull()].index.tolist()
        if empty_rows:
            empty_data[column] = [i + 2 for i in empty_rows]

    if empty_data:
        return {
            "status": 0,
            "message": "El archivo contiene campos vacíos en las siguientes columnas:",
            "empty_columns": list(empty_data.keys()),
            "empty_cells": empty_data
        }

    session = get_db_connection()
    errores = []

    try:
        cursor = session.cursor()
        hoy = datetime.now().strftime("%Y-%m-%d")

        # Eliminar historial del día actual
        cursor.execute("""
            DELETE FROM apl_imperio.APP_SALESFORCE_Historial 
            WHERE CONVERT(DATE, fecha_backup) = ?
        """, (hoy,))

        # Truncar tabla principal
        cursor.execute("TRUNCATE TABLE apl_imperio.APP_SALESFORCE_Dataframe")

        # Insertar cada fila del DataFrame
        for index, row in df.iterrows():
            try:
                if pd.isna(row['Fecha']):
                    raise ValueError("Fecha vacía")
                row['Seller_ID'] = str(row['Seller_ID'])                
                if 'Cita' in row.index:  # Verifica si 'Cita' está en las columnas
                    cita_valor = row['Cita']
                    if cita_valor is None or str(cita_valor).strip() in ["", "-", "'-", "nan", "None"]:
                        cita_convertido = None
                    else:
                        try:
                            cita_convertido = int(float(str(cita_valor).strip()))
                        except Exception as cita_error:
                            raise ValueError(f"Error en campo 'Cita' con valor '{cita_valor}': {cita_error}")
                else:
                    cita_convertido = None
                valores = (
                    row['Fecha'], row['Seller_ID'], row['Seller'], row['Placa'],
                    row['Flujo'], cita_convertido, nombre_flujo, fecha_carga, hora_carga
                )

                cursor.execute("""
                    INSERT INTO apl_imperio.APP_SALESFORCE_Dataframe
                    (fecha, Seller_ID, Seller, Placa, Flujo, Cita, nombre_flujo, fecha_carga, hora_carga)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, valores)

            except Exception as e:
                mensaje_error = str(e)
                columna_error, valor_error = identificar_columna_y_valor_error(mensaje_error, row)
                errores.append({
                    "fila": index + 2,
                    "detalle": mensaje_error,
                    "fila_contenido": row.to_dict(),
                    "columna_problematica": columna_error,
                    "valor_problematico": valor_error
                })
                print("❌ Error en fila", index + 2, ":", mensaje_error)
                raise

        # Insertar en historial
        cursor.execute("""
            INSERT INTO apl_imperio.APP_SALESFORCE_HISTORIAL (
                fecha, Seller_ID, Seller, Placa, Flujo, Cita,
                nombre_flujo, fecha_carga, hora_carga, id_carga
            )
            SELECT fecha, Seller_ID, Seller, Placa, Flujo, Cita,
                   nombre_flujo, fecha_carga, hora_carga, id_carga
            FROM apl_imperio.APP_SALESFORCE_Dataframe
        """)

        session.commit()

        return {
            "status": 1,
            "message": "Todos los registros fueron insertados correctamente."
        }

    except Exception as e:
        session.rollback()
        print("❌ Error general:", str(e))
        print("📛 Traceback:")
        print(traceback.format_exc())
        return {
            "status": 0,
            "message": "❌ Ocurrió un error y no se insertó nada.",
            "errores": errores,
            "detalle": str(e),
            "trace": traceback.format_exc()
        }

    finally:
        session.close()
        
def encontrar_columna_similar(df, referencia="Cita", umbral=0.75):
    for col in df.columns:
        ratio = SequenceMatcher(None, referencia, col).ratio()
        if ratio >= umbral:
            return col
    return None
        
def identificar_columna_y_valor_error(mensaje_error, fila):
    """
    Identifica la columna y el valor que están causando el error.
    
    Args:
        mensaje_error: Mensaje de error capturado
        fila: Fila de datos que estaba siendo procesada cuando ocurrió el error
        
    Returns:
        tuple: (columna_error, valor_error)
    """
    # Columnas a verificar y sus valores en la fila
    columnas = {'Seller_ID': fila['Seller_ID'], 
                'Seller': fila['Seller'], 
                'Placa': fila['Placa'], 
                'Flujo': fila['Flujo'], 
                'Cita': fila['Cita'],
                'Fecha': fila['Fecha']}
    
    # Intentar extraer el valor problemático del mensaje de error
    valor_problema = None
    match = re.search(r"value '([^']+)'", mensaje_error)
    if match:
        valor_problema = match.group(1)
        
        # Si encontramos el valor problemático, buscamos en qué columna está
        for columna, valor in columnas.items():
            # Convertir ambos a string para comparación
            if str(valor) == valor_problema:
                return columna, valor_problema
    
    # Si no encontramos el valor, buscamos por nombre de columna en el mensaje
    for columna in columnas.keys():
        if columna in mensaje_error:
            return columna, columnas[columna]
    
    # Si aún no encontramos, intentamos con otros patrones
    if "Seller_ID" in mensaje_error or "SellerID" in mensaje_error:
        return "Seller_ID", columnas["Seller_ID"]
    elif "Placa" in mensaje_error:
        return "Placa", columnas["Placa"]
    elif "fecha" in mensaje_error.lower() or "date" in mensaje_error.lower():
        return "Fecha", columnas["Fecha"]
    elif "Cita" in mensaje_error:
        return "Cita", columnas["Cita"]
    
    # No se identifica la columna específica
    return "No identificada", valor_problema if valor_problema else "Desconocido"

@app.get("/datos-actualizados")
async def actualizarDatos(
    page: int = 1,
    size: int = 10
):
    session = get_db_connection()
    try:
        with session.cursor() as cursor:
            # Obtener la última fecha disponible con datos
            cursor.execute("""
                SELECT TOP 1 fecha_carga 
                FROM apl_imperio.APP_SALESFORCE_Dataframe 
                ORDER BY fecha_carga DESC
            """)
            row = cursor.fetchone()
            if not row:
                return {"status": 0, "message": "No hay datos disponibles"}

            fecha_consulta = row[0]
            print("Usando fecha:", fecha_consulta)

            # Query para contar el total de registros
            cursor.execute("""
                SELECT COUNT(*) as total 
                FROM apl_imperio.APP_SALESFORCE_Dataframe 
                WHERE fecha_carga = ?
            """, (fecha_consulta,))
            total = cursor.fetchone()[0]

            # Query paginada
            offset = (page - 1) * size
            cursor.execute("""
                SELECT * FROM apl_imperio.APP_SALESFORCE_Dataframe 
                WHERE fecha_carga = ?
                ORDER BY id_carga 
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """, (fecha_consulta, offset, size))

            resultados = cursor.fetchall()
            columnas = [column[0] for column in cursor.description]
            datos = [dict(zip(columnas, fila)) for fila in resultados]

            total_pages = (total + size - 1) // size

            links = []
            if page > 1:
                links.append({"url": f"/datos-actualizados?page={page-1}&size={size}", "label": "«", "active": False})
            else:
                links.append({"url": None, "label": "«", "active": False})

            for i in range(1, total_pages + 1):
                links.append({
                    "url": f"/datos-actualizados?page={i}&size={size}",
                    "label": str(i),
                    "active": i == page
                })

            if page < total_pages:
                links.append({"url": f"/datos-actualizados?page={page+1}&size={size}", "label": "»", "active": False})
            else:
                links.append({"url": None, "label": "»", "active": False})

            return {
                "status": 1,
                "fecha": fecha_consulta,
                "datos": {
                    "data": datos,
                    "current_page": page,
                    "per_page": size,
                    "total": total,
                    "links": links
                }
            }

    except Exception as e:
        return {"status": 0, "error": str(e)}
    finally:
        session.close()
        
@app.get("/datos")
async def datos(flujos: Optional[List[str]] = Query(None)):
    print("flujos recibidos:", flujos)
    placeholders = ','.join(['?'] * len(flujos))
    fecha_hoy = datetime.today().strftime('%Y-%m-%d') if flujos else ''
    session = get_db_connection()
    try:
        with session.cursor() as cursor:
            if flujos and len(flujos) > 0:
                query = f"""SELECT * FROM apl_imperio.APP_SALESFORCE_Dataframe where fecha_carga = ? AND Flujo IN ({placeholders})"""
                fecha_hoy = date.today().isoformat()
                params = [fecha_hoy] + flujos
                print("params is: ", params)
            else:
                query = f"""SELECT * FROM apl_imperio.APP_SALESFORCE_Dataframe where fecha_carga = ? """
                params = [fecha_hoy]
            cursor.execute(query,params)
            resultados = cursor.fetchall()
            columnas = [column[0] for column in cursor.description]  # Obtener los nombres de las columnas
            datos = [dict(zip(columnas, fila)) for fila in resultados]  # Convertir filas a diccionarios
            if resultados:
                return{"datos":datos}
            else:
                return {"flujos": flujos}
    except Exception as e:
        return{"status":0,"error":str(e)}
    finally:
        session.close()
@app.get("/datosPersonal/{flujo}")
async def datos(flujo:str):
    session = get_db_connection()
    try:
        with session.cursor() as cursor:
            fecha_hoy = datetime.today().strftime('%Y-%m-%d')
            cursor.execute("""SELECT * FROM apl_imperio.APP_SALESFORCE_Personal where fecha_carga = ? and flujo = ? """,(fecha_hoy,flujo))
            resultados = cursor.fetchall()
            columnas = [column[0] for column in cursor.description]  # Obtener los nombres de las columnas
            datos = [dict(zip(columnas, fila)) for fila in resultados]  # Convertir filas a diccionarios
            if resultados:
                return{"datos":datos}
            else:
                return{"status":0,"message":"No hay datos"}
    except Exception as e:
        return{"status":0,"error":str(e)}
    finally:
        session.close()
        
@app.get("/datos_actualizados/{id_carga}")
async def actualizarDatos(id_carga:int):
    session = get_db_connection()
    try:
        with session.cursor() as cursor:
            fecha_hoy = datetime.today().strftime('%Y-%m-%d')
            cursor.execute("""SELECT * FROM apl_imperio.APP_SALESFORCE_Dataframe where fecha_carga = ? and id_carga = ?""",(fecha_hoy,id_carga,))
            resultados = cursor.fetchall()
            columnas = [column[0] for column in cursor.description]  # Obtener los nombres de las columnas
            datos = [dict(zip(columnas, fila)) for fila in resultados]  # Convertir filas a diccionarios
            if resultados:
                return{"status":1,"datos":datos}
            else:
                return{"status":0,"message":"No hay datos"}
    except Exception as e:
        print("Entro aqui")
        return{"status":0,"error":str(e)}
    finally:
        session.close()
        
        
@app.get("/datos_actualizados_personal/{id_carga}/{RUC}")
async def actualizarDatos(id_carga:int,RUC:str):
    session = get_db_connection()
    try:
        with session.cursor() as cursor:
            fecha_hoy = datetime.today().strftime('%Y-%m-%d')
            cursor.execute("""SELECT * FROM apl_imperio.APP_SALESFORCE_Personal where fecha_carga = ? and id_carga = ? AND ruc = ?""",(fecha_hoy,id_carga,RUC))
            resultados = cursor.fetchall()
            columnas = [column[0] for column in cursor.description]  # Obtener los nombres de las columnas
            datos = [dict(zip(columnas, fila)) for fila in resultados]  # Convertir filas a diccionarios
            if resultados:
                return{"status":1,"datos":datos}
            else:
                return{"status":0,"message":"No hay datos"}
    except Exception as e:
        print("Entro aqui")
        return{"status":0,"error":str(e)}
    finally:
        session.close()
        
        
@app.get("/datos-actualizados-personal/{RUC}")
async def actualizarDatos(
    RUC: str,
    page: int = 1,  # Parámetro de consulta para la página actual
    size: int = 10  # Parámetro de consulta para elementos por página
):
    session = get_db_connection()
    try:
        with session.cursor() as cursor:
            fecha_hoy = datetime.today().strftime('%Y-%m-%d')
            
            # Query para contar el total de registros
            print("El ruc es",RUC)
            cursor.execute("""
                SELECT COUNT(*) as total FROM apl_imperio.APP_SALESFORCE_Personal 
                WHERE fecha_carga = ? and RUC = ?
            """, (fecha_hoy,RUC,))
            total = cursor.fetchone()[0]
            
            # Query paginada
            offset = (page - 1) * size
            cursor.execute("""
                SELECT * FROM apl_imperio.APP_SALESFORCE_Personal
                WHERE fecha_carga = ? and RUC = ?
                ORDER BY id_carga 
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """, (fecha_hoy,RUC, offset, size))
            
            resultados = cursor.fetchall()
            columnas = [column[0] for column in cursor.description]
            datos = [dict(zip(columnas, fila)) for fila in resultados]
            
            # Calcular total de páginas
            total_pages = (total + size - 1) // size
            
            # Crear enlaces de paginación
            links = []
            # Agregar enlace para la página anterior
            base_url = f"/datos-actualizados-personal/{RUC}"
            if page > 1:
                links.append({ "url": f"{base_url}?page={page - 1}&size={size}", "label": "«", "active": False})
            else:
                links.append({"url": None, "label": "«", "active": False})
            
            # Agregar enlaces para cada página
            for i in range(1, total_pages + 1):
                links.append({
                    "url": f"{base_url}?page={i}&size={size}",
                    "label": str(i),
                    "active": i == page
                })
            
            # Agregar enlace para la página siguiente
            if page < total_pages:
                links.append({"url": f"{base_url}?page={i}&size={size}", "label": "»", "active": False})
            else:
                links.append({"url": None, "label": "»", "active": False})
            
            if resultados:
                return {
                    "status": 1,
                    "datos": {
                        "data": datos,
                        "current_page": page,
                        "per_page": size,
                        "total": total,
                        "links": links
                    }
                }
            else:
                return {"status": 0, "message": "No hay datos"}
            
    except Exception as e:
        return {"status": 0, "error": str(e)}
    finally:
        session.close()