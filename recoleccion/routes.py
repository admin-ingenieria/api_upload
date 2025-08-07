from fastapi import File, UploadFile, Form
import pandas as pd
from datetime import date, time,datetime
from io import BytesIO
import traceback
from sqlalchemy import text
from db import get_db_connection
import re
from fastapi import APIRouter
reco_router = APIRouter()


@reco_router.post("/personal_excel")
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
    columnas = {'PICKUP': fila['PICKUP'], 
                'TIPO': fila['TIPO'], 
                'PLACA': fila['PLACA'], 
                'NOMBRES': fila['NOMBRES'], 
                'DOCUMENTO': fila['DOCUMENTO'],
                'CARGO': fila['CARGO'],
                'EMPRESA': fila['EMPRESA'],
                'RUC': fila['RUC']}
    
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
    if "PICKUP" in mensaje_error:
        return "PICKUP", columnas["PICKUP"]
    elif "TIPO" in mensaje_error:
        return "TIPO", columnas["TIPO"]
    elif "PLACA" in mensaje_error.lower() or "date" in mensaje_error.lower():
        return "PLACA", columnas["PLACA"]
    elif "NOMBRES" in mensaje_error:
        return "NOMBRES", columnas["NOMBRES"]
    elif "DOCUMENTO" in mensaje_error:
        return "DOCUMENTO", columnas['DOCUMENTO']
    elif "CARGO" in mensaje_error:
        return "CARGO", columnas['CARGO']
    elif "EMPRESA" in mensaje_error:
        return "EMPRESA", columnas["EMPRESA"]
    elif "RUC" in mensaje_error:
        return "RUC",columnas['RUC']
    
    # No se identifica la columna específica
    return "No identificada", valor_problema if valor_problema else "Desconocido"

@reco_router.get("/datosPersonal/{flujo}")
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

@reco_router.get("/datos_actualizados_personal/{id_carga}/{RUC}")
async def actualizarDatos(id_carga:int,RUC:str):
    session = get_db_connection()
    try:
        with session.cursor() as cursor:
            fecha_hoy = datetime.today().strftime('%Y-%m-%d')
            cursor.execute("""SELECT * FROM apl_imperio.APP_SALESFORCE_Personal where id_carga = ? AND ruc = ?""",(id_carga,RUC))
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
        
        
@reco_router.get("/datos-actualizados-personal/{RUC}")
async def actualizarDatos(
    RUC: str,
    page: int = 1,  # Parámetro de consulta para la página actual
    size: int = 10  # Parámetro de consulta para elementos por página
):
    session = get_db_connection()
    try:
        with session.cursor() as cursor:
            fecha_hoy = datetime.today().strftime('%Y-%m-%d')
            #Comprabamos si hoy se subio data
            cursor.execute("""SELECT COUNT(*) as cantidad
            FROM apl_imperio.APP_SALESFORCE_Personal
            WHERE fecha_carga = ? AND RUC = ?""",(fecha_hoy,RUC))
            result = cursor.fetchone()[0]
            if result == 0:
                #Obtenemos la ultima fecha
                cursor.execute("""SELECT MAX(fecha_carga) AS ultima_fecha FROM apl_imperio.APP_SALESFORCE_Personal WHERE RUC = ?""",(RUC,))
                ultima_fecha = cursor.fetchone()[0]
                fecha_hoy = ultima_fecha
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
