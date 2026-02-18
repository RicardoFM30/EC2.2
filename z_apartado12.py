import boto3
import time
import csv
import os

# ---------------- CONFIGURACIÓN ---------------- #
BUCKET = 'mi-bucket-athena'                  # tu bucket S3
PARTICIONES_PATH = 'particiones'             # carpeta base para particiones
OUTPUT = f's3://{BUCKET}/resultados/'
DATABASE = 'mi_basedatos_particion'
TABLE_NAME = 'mi_tabla_particion'

s3 = boto3.client('s3')
athena = boto3.client('athena')

# ---------------- FUNCIONES ---------------- #

def crear_csv_local(nombre_archivo, datos):
    """Crea un CSV local con los datos proporcionados"""
    with open(nombre_archivo, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'nombre', 'edad'])
        writer.writerows(datos)
    print(f"CSV creado: {nombre_archivo}")

def subir_csv_particion(bucket, archivo_local, anio):
    """Sube CSV a S3 dentro de la carpeta de partición anio=xxxx"""
    key = f'{PARTICIONES_PATH}/anio={anio}/{archivo_local}'
    s3.upload_file(archivo_local, bucket, key)
    print(f"Subido a S3: {key}")

def crear_base_datos():
    query = f"CREATE DATABASE IF NOT EXISTS {DATABASE};"
    athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={'OutputLocation': OUTPUT}
    )
    print("Base de datos creada.")

def crear_tabla_particionada():
    query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {TABLE_NAME} (
        id INT,
        nombre STRING,
        edad INT
    )
    PARTITIONED BY (anio INT)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'quoteChar' = '"'
    )
    LOCATION 's3://{BUCKET}/{PARTICIONES_PATH}/'
    TBLPROPERTIES ('skip.header.line.count'='1');
    """
    athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': OUTPUT}
    )
    print("Tabla particionada creada.")

def agregar_particiones():
    query = f"MSCK REPAIR TABLE {TABLE_NAME};"
    athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': OUTPUT}
    )
    print("Particiones cargadas en Athena.")

def ejecutar_query(query):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': OUTPUT}
    )
    query_id = response['QueryExecutionId']

    while True:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        estado = result['QueryExecution']['Status']['State']
        if estado in ['SUCCEEDED','FAILED','CANCELLED']:
            break
        time.sleep(2)

    if estado == 'SUCCEEDED':
        return athena.get_query_results(QueryExecutionId=query_id)
    else:
        print("Error en consulta:", estado)
        return None

def mostrar_resultados(resultados):
    if not resultados:
        return
    for row in resultados['ResultSet']['Rows']:
        print([col.get('VarCharValue','') for col in row['Data']])

# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    print("🚀 INICIO PROCESO ATHENA TABLA PARTICIONADA AUTONOMA")

    # 1️⃣ Crear CSVs locales y subirlos a S3 por partición
    datos_2023 = [
        [1,'Ana',30],
        [2,'Luis',25]
    ]
    datos_2024 = [
        [3,'Carlos',40],
        [4,'Laura',28]
    ]

    crear_csv_local('datos_2023.csv', datos_2023)
    crear_csv_local('datos_2024.csv', datos_2024)

    subir_csv_particion(BUCKET, 'datos_2023.csv', 2023)
    subir_csv_particion(BUCKET, 'datos_2024.csv', 2024)

    # 2️⃣ Crear base de datos y tabla
    crear_base_datos()
    crear_tabla_particionada()

    # 3️⃣ Cargar particiones
    agregar_particiones()

    # 4️⃣ Consulta sobre partición específica
    print("\n--- CONSULTA: Personas del año 2023 ---")
    q = f"SELECT * FROM {TABLE_NAME} WHERE anio = 2023;"
    res = ejecutar_query(q)
    mostrar_resultados(res)

    print("\n🎉 PROCESO TABLA PARTICIONADA COMPLETO FINALIZADO")
