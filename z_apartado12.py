import time
import csv
import os
from aws_session import get_s3_client, get_athena_client

# ---------------- CONFIGURACIÓN ---------------- #
BUCKET = 'mibuckeriaricardoathenacolumnaparticionada'                  
PARTICIONES_PATH = 'particiones'             
OUTPUT = f's3://{BUCKET}/resultados/'
DATABASE = 'mi_basedatos_particion'
TABLE_NAME = 'mi_tabla_particion'

# ---------------- CLIENTES ---------------- #
s3 = get_s3_client()
athena = get_athena_client()


def ensure_bucket_exists(bucket):
    """Verifica que el bucket exista y lo crea si no."""
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' existe.")
    except Exception:
        print(f"Bucket '{bucket}' no existe, creándolo...")
        region = os.getenv('REGION', 'us-east-1')
        try:
            if region == 'us-east-1':
                s3.create_bucket(Bucket=bucket)
            else:
                s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={'LocationConstraint': region})
            print(f"Bucket '{bucket}' creado.")
        except Exception as e:
            print(f"Error creando bucket '{bucket}': {e}")
            raise

# ---------------- FUNCIONES ---------------- #

def crear_csv_local(nombre_archivo, datos):
    """Crea un CSV local con los datos proporcionados"""
    with open(nombre_archivo, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'nombre', 'edad'])
        writer.writerows(datos)
    print(f"✅ CSV creado: {nombre_archivo}")

def subir_csv_particion(bucket, archivo_local, anio):
    """Sube CSV a S3 dentro de la carpeta de partición anio=xxxx"""
    key = f'{PARTICIONES_PATH}/anio={anio}/{archivo_local}'
    s3.upload_file(archivo_local, bucket, key)
    print(f"✅ Subido a S3: {key}")

def ejecutar_query(query, database=DATABASE):
    """Ejecuta query en Athena y espera a que termine"""
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': OUTPUT}
    )
    qid = response['QueryExecutionId']
    while True:
        st = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']
        state = st['State']
        if state in ['SUCCEEDED','FAILED','CANCELLED']:
            break
        time.sleep(1)
    if state != 'SUCCEEDED':
        reason = st.get('StateChangeReason', '')
        print(f"❌ Error en query: {state} - {reason}")
        return None
    return athena.get_query_results(QueryExecutionId=qid)

def mostrar_resultados(resultados):
    """Imprime resultados legibles"""
    if not resultados:
        return
    for row in resultados['ResultSet']['Rows']:
        print([col.get('VarCharValue','') for col in row['Data']])

def crear_base_datos():
    q = f"CREATE DATABASE IF NOT EXISTS {DATABASE};"
    ejecutar_query(q)
    print(f"✅ Base de datos '{DATABASE}' creada o ya existía")

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
        'quoteChar' = '"',
        'use.null.for.invalid.data'='true'
    )
    LOCATION 's3://{BUCKET}/{PARTICIONES_PATH}/'
    TBLPROPERTIES ('skip.header.line.count'='1');
    """
    ejecutar_query(query)
    print(f"✅ Tabla particionada '{TABLE_NAME}' creada o ya existía")

def agregar_particiones():
    query = f"MSCK REPAIR TABLE {TABLE_NAME};"
    ejecutar_query(query)
    print("✅ Particiones cargadas en Athena")

# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    print("🚀 INICIO PROCESO ATHENA TABLA PARTICIONADA")

    # Asegurar que el bucket existe
    ensure_bucket_exists(BUCKET)

    # 1️⃣ Crear CSVs locales y subirlos a S3 por partición
    datos_2023 = [[1,'Ana',30],[2,'Luis',25]]
    datos_2024 = [[3,'Carlos',40],[4,'Laura',28]]

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
    res = ejecutar_query(f"SELECT * FROM {TABLE_NAME} WHERE anio=2023;")
    mostrar_resultados(res)

    print("\n🎉 PROCESO TABLA PARTICIONADA COMPLETO FINALIZADO")