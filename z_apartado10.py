import boto3
import time
import os

# ---------------- CONFIGURACIÓN ---------------- #
DATABASE = 'mi_basedatos'                       # Base de datos Athena
BUCKET = 'mi-bucket-athena'                     # Bucket donde está el CSV
CSV_PATH = f's3://{BUCKET}/datos.csv'           # Ruta del CSV
OUTPUT = f's3://{BUCKET}/resultados/'           # Carpeta donde Athena guardará resultados
TABLE_NAME = 'mi_tabla_csv'                     # Nombre de la tabla Athena

athena = boto3.client('athena')


# ---------------- FUNCIONES ---------------- #

def crear_base_datos():
    """Crea la base de datos si no existe"""
    query = f"CREATE DATABASE IF NOT EXISTS {DATABASE};"
    response = athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={'OutputLocation': OUTPUT}
    )
    print("Base de datos creada (si no existía). QueryId:", response['QueryExecutionId'])


def crear_tabla():
    """Crea la tabla externa sobre el CSV en S3"""
    query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {TABLE_NAME} (
        id INT,
        nombre STRING,
        edad INT
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'quoteChar' = '"'
    )
    LOCATION '{CSV_PATH.rsplit('/',1)[0]}/'
    TBLPROPERTIES ('skip.header.line.count'='1');
    """

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': OUTPUT}
    )

    print("Tabla creada (si no existía). QueryId:", response['QueryExecutionId'])


def ejecutar_query(query):
    """Ejecuta la consulta y espera hasta que termine"""
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': OUTPUT}
    )

    query_id = response['QueryExecutionId']

    # Espera hasta que termine
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        estado = result['QueryExecution']['Status']['State']

        if estado in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)

    if estado == 'SUCCEEDED':
        resultados = athena.get_query_results(QueryExecutionId=query_id)
        return resultados
    else:
        print("Error en consulta:", estado)
        return None


def mostrar_resultados(resultados):
    """Muestra los resultados de forma legible"""
    if not resultados:
        return
    for row in resultados['ResultSet']['Rows']:
        valores = [col.get('VarCharValue', '') for col in row['Data']]
        print(valores)


# ---------------- MAIN ---------------- #

if __name__ == "__main__":

    print("🚀 INICIO DEL PROCESO ATHENA CON CSV EN S3")

    # 1️⃣ Crear base de datos
    crear_base_datos()

    # 2️⃣ Crear tabla
    crear_tabla()

    # 3️⃣ Consulta 1: todos los datos
    print("\n--- CONSULTA 1: TODOS LOS DATOS ---")
    q1 = f"SELECT * FROM {TABLE_NAME};"
    res1 = ejecutar_query(q1)
    mostrar_resultados(res1)

    # 4️⃣ Consulta 2: filtrar edad > 30
    print("\n--- CONSULTA 2: edad > 30 ---")
    q2 = f"SELECT * FROM {TABLE_NAME} WHERE edad > 30;"
    res2 = ejecutar_query(q2)
    mostrar_resultados(res2)

    # 5️⃣ Consulta 3: conteo total
    print("\n--- CONSULTA 3: COUNT ---")
    q3 = f"SELECT COUNT(*) FROM {TABLE_NAME};"
    res3 = ejecutar_query(q3)
    mostrar_resultados(res3)

    print("\n🎉 PROCESO ATHENA FINALIZADO")
