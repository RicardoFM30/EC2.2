import time
import csv
from faker import Faker
from aws_session import get_athena_client, get_s3_client

# ---------------- CONFIGURACIÓN ---------------- #
DATABASE = 'mi_basedatos'
BUCKET = 'mibuckeriaricardoathena'
TABLE_NAME = 'mi_tabla_csv'
CSV_LOCAL = "datos.csv"
CSV_S3_KEY = "datos.csv"
OUTPUT = f"s3://{BUCKET}/resultados/"

# ---------------- CLIENTES AWS ---------------- #
s3 = get_s3_client()
athena = get_athena_client()

# ---------------- FUNCIONES ---------------- #

def limpiar_resultados_anteriores():
    """Eliminar resultados antiguos de Athena en el bucket"""
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix='resultados/')
    if 'Contents' in response:
        for obj in response['Contents']:
            s3.delete_object(Bucket=BUCKET, Key=obj['Key'])
    print("✅ Resultados antiguos eliminados")

def crear_bucket_si_no_existe():
    """Crea bucket si no existe"""
    try:
        s3.head_bucket(Bucket=BUCKET)
        print(f"Bucket '{BUCKET}' existe.")
    except Exception:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Bucket '{BUCKET}' creado.")

def generar_csv():
    """Genera CSV de prueba y lo sube a S3"""
    fake = Faker()
    with open(CSV_LOCAL, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'nombre', 'edad'])
        for i in range(1, 201):
            writer.writerow([i, fake.name(), fake.random_int(18, 80)])
    s3.upload_file(Filename=CSV_LOCAL, Bucket=BUCKET, Key=CSV_S3_KEY)
    print(f"CSV de prueba generado y subido a s3://{BUCKET}/{CSV_S3_KEY}")

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
        if st['State'] in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            break
        time.sleep(1)
    if st['State'] != 'SUCCEEDED':
        print(f"❌ Error en query: {st['State']} - {st.get('StateChangeReason')}")
        return None
    return athena.get_query_results(QueryExecutionId=qid)

def mostrar_resultados(resultados):
    if not resultados:
        return
    for row in resultados['ResultSet']['Rows']:
        print([col.get('VarCharValue', '') for col in row['Data']])

def crear_base_datos():
    ejecutar_query(f"CREATE DATABASE IF NOT EXISTS {DATABASE};")
    print(f"✅ Base de datos '{DATABASE}' creada o ya existía")

def crear_tabla():
    query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {TABLE_NAME} (
        id INT,
        nombre STRING,
        edad INT
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'quoteChar' = '"',
        'use.null.for.invalid.data' = 'true'
    )
    LOCATION 's3://{BUCKET}/'
    TBLPROPERTIES ('skip.header.line.count'='1');
    """
    ejecutar_query(query)
    print(f"✅ Tabla '{TABLE_NAME}' creada o ya existía")

# ---------------- MAIN ---------------- #

if __name__ == "__main__":
    print("🚀 INICIO DEL PROCESO ATHENA CON CSV EN S3")

    crear_bucket_si_no_existe()
    limpiar_resultados_anteriores()
    generar_csv()
    crear_base_datos()
    crear_tabla()
    time.sleep(2)  # Pequeña pausa para asegurar que todo esté listo
    
    print("\n--- CONSULTA 1: Contar todas las entradas de la base de datos ---")
    mostrar_resultados(ejecutar_query(f"SELECT COUNT(*) FROM {TABLE_NAME};"))

    print("\n--- CONSULTA 2: Consulta simple para listar las personas con mas de 30 ---")
    mostrar_resultados(ejecutar_query(f"SELECT * FROM {TABLE_NAME} WHERE edad > 30;"))

    print("\n--- CONSULTA 3: Promedio de edades de las personas ---")
    mostrar_resultados(ejecutar_query(f"SELECT AVG(edad) AS edad_promedio FROM {TABLE_NAME};"))


    print("\n🎉 PROCESO ATHENA FINALIZADO")