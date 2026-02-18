import boto3
import json
import time
import os

# ---------------- CONFIGURACIÓN ---------------- #
DATABASE = 'mi_basedatos_json'
BUCKET = 'mi-bucket-athena'  # tu bucket S3
JSON_FOLDER = 'json'          # carpeta dentro del bucket para el JSON
OUTPUT = f's3://{BUCKET}/resultados/'
TABLE_NAME = 'mi_tabla_json'

s3 = boto3.client('s3')
athena = boto3.client('athena')

# ---------------- FUNCIONES ---------------- #

def crear_json_local(nombre_archivo):
    """Crea un archivo JSON de ejemplo localmente"""
    datos = [
        {"id": 1, "nombre": "Ana", "edad": 30, "ciudad": "Madrid"},
        {"id": 2, "nombre": "Luis", "edad": 25, "ciudad": "Barcelona"},
        {"id": 3, "nombre": "Carlos", "edad": 40, "ciudad": "Valencia"},
        {"id": 4, "nombre": "Laura", "edad": 28, "ciudad": "Sevilla"}
    ]
    with open(nombre_archivo, 'w') as f:
        for fila in datos:
            f.write(json.dumps(fila) + "\n")  # JSON Lines
    print(f"JSON local creado: {nombre_archivo}")

def subir_json_s3(local_file):
    """Sube el JSON al bucket S3"""
    key = f"{JSON_FOLDER}/{local_file}"
    s3.upload_file(local_file, BUCKET, key)
    print(f"JSON subido a S3: {key}")
    return f"s3://{BUCKET}/{JSON_FOLDER}/"

def crear_base_datos():
    """Crea la base de datos en Athena si no existe"""
    query = f"CREATE DATABASE IF NOT EXISTS {DATABASE};"
    athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={'OutputLocation': OUTPUT}
    )
    print("Base de datos creada.")

def crear_tabla_json(json_s3_path):
    """Crea la tabla externa apuntando al JSON en S3"""
    query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {TABLE_NAME} (
        id INT,
        nombre STRING,
        edad INT,
        ciudad STRING
    )
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    LOCATION '{json_s3_path}';
    """
    athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': OUTPUT}
    )
    print("Tabla JSON creada.")

def ejecutar_query(query):
    """Ejecuta consulta en Athena y espera resultado"""
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': OUTPUT}
    )
    query_id = response['QueryExecutionId']

    while True:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        estado = result['QueryExecution']['Status']['State']
        if estado in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)

    if estado == 'SUCCEEDED':
        return athena.get_query_results(QueryExecutionId=query_id)
    else:
        print("Error en consulta:", estado)
        return None

def mostrar_resultados(resultados):
    """Imprime los resultados de Athena de manera legible"""
    if not resultados:
        return
    for row in resultados['ResultSet']['Rows']:
        print([col.get('VarCharValue', '') for col in row['Data']])

# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    print("🚀 INICIO PROCESO ATHENA CON JSON AUTONOMO")

    # 1️⃣ Crear JSON local
    crear_json_local('datos.json')

    # 2️⃣ Subir JSON a S3
    json_s3_path = subir_json_s3('datos.json')

    # 3️⃣ Crear base de datos
    crear_base_datos()

    # 4️⃣ Crear tabla en Athena apuntando al JSON
    crear_tabla_json(json_s3_path)

    # 5️⃣ Consulta 1: todos los registros
    print("\n--- CONSULTA 1: TODOS ---")
    q1 = f"SELECT * FROM {TABLE_NAME};"
    res1 = ejecutar_query(q1)
    mostrar_resultados(res1)

    # 6️⃣ Consulta 2: filtrar edad > 30
    print("\n--- CONSULTA 2: EDAD > 30 ---")
    q2 = f"SELECT nombre, ciudad FROM {TABLE_NAME} WHERE edad > 30;"
    res2 = ejecutar_query(q2)
    mostrar_resultados(res2)

    # 7️⃣ Consulta 3: contar personas por ciudad
    print("\n--- CONSULTA 3: COUNT POR CIUDAD ---")
    q3 = f"SELECT ciudad, COUNT(*) AS total FROM {TABLE_NAME} GROUP BY ciudad;"
    res3 = ejecutar_query(q3)
    mostrar_resultados(res3)

    print("\n🎉 PROCESO ATHENA JSON AUTONOMO FINALIZADO")
