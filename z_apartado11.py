import time
import os
import json
from faker import Faker
from aws_session import get_athena_client, get_s3_client

# ---------------- CONFIGURACIÓN ---------------- #
DATABASE_JSON = 'mi_basedatos_json'
BUCKET_JSON = 'mibuckeriaricardoathenajson'
TABLE_JSON = 'mi_tabla_json'

TIMESTAMP_JSON = int(time.time())
DATA_FOLDER_JSON = f"datos_json_{TIMESTAMP_JSON}"
RESULT_FOLDER_JSON = f"resultados_json_{TIMESTAMP_JSON}"

JSON_LOCAL = "datos.json"
JSON_S3_PATH = f"s3://{BUCKET_JSON}/{DATA_FOLDER_JSON}/datos.json"
OUTPUT_JSON = f"s3://{BUCKET_JSON}/{RESULT_FOLDER_JSON}/"

# ---------------- CLIENTES AWS ---------------- #
s3 = get_s3_client()
athena = get_athena_client()

# ---------------- FUNCIONES ---------------- #

def crear_bucket_si_no_existe():
    """Crea bucket si no existe"""
    try:
        s3.head_bucket(Bucket=BUCKET_JSON)
        print(f"Bucket '{BUCKET_JSON}' existe.")
    except Exception:
        region = os.getenv('REGION', 'us-east-1')
        if region == 'us-east-1':
            s3.create_bucket(Bucket=BUCKET_JSON)
        else:
            s3.create_bucket(Bucket=BUCKET_JSON,
                             CreateBucketConfiguration={'LocationConstraint': region})
        print(f"Bucket '{BUCKET_JSON}' creado.")

def generar_json():
    """Genera archivo JSON de prueba y lo sube a S3"""
    fake = Faker()
    os.makedirs(DATA_FOLDER_JSON, exist_ok=True)
    local_path = os.path.join(DATA_FOLDER_JSON, JSON_LOCAL)
    data = []
    for i in range(1, 201):
        data.append({
            "id": i,
            "nombre": fake.name(),
            "edad": fake.random_int(min=18, max=80),
            "ciudad": fake.city()
        })
    with open(local_path, "w", encoding="utf-8") as f:
        for record in data:
            f.write(json.dumps(record) + "\n")  # JSON por línea
    s3.upload_file(Filename=local_path, Bucket=BUCKET_JSON, Key=f"{DATA_FOLDER_JSON}/{JSON_LOCAL}")
    print(f"JSON de prueba generado y subido a s3://{BUCKET_JSON}/{DATA_FOLDER_JSON}/{JSON_LOCAL}")

def ejecutar_query(query, database=DATABASE_JSON):
    """Ejecuta query en Athena y espera a que termine"""
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': OUTPUT_JSON}
    )
    qid = response['QueryExecutionId']
    while True:
        st = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']
        state = st['State']
        if state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            break
        time.sleep(1)
    if state != 'SUCCEEDED':
        reason = st.get('StateChangeReason')
        print(f"❌ Error en query: {state} - {reason}")
        return None
    return athena.get_query_results(QueryExecutionId=qid)

def mostrar_resultados(resultados):
    """Imprime resultados legibles"""
    if not resultados:
        return
    for row in resultados['ResultSet']['Rows']:
        valores = [col.get('VarCharValue', '') for col in row['Data']]
        print(valores)

def crear_base_datos_json():
    query = f"CREATE DATABASE IF NOT EXISTS {DATABASE_JSON};"
    ejecutar_query(query)
    print(f"✅ Base de datos '{DATABASE_JSON}' creada o ya existía")

def crear_tabla_json():
    """Crea tabla externa apuntando al JSON"""
    query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {TABLE_JSON} (
        id INT,
        nombre STRING,
        edad INT,
        ciudad STRING
    )
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    LOCATION 's3://{BUCKET_JSON}/{DATA_FOLDER_JSON}/'
    TBLPROPERTIES ('ignore.malformed.json'='true');
    """
    ejecutar_query(query)
    print(f"✅ Tabla '{TABLE_JSON}' creada o ya existía")

# ---------------- MAIN ---------------- #

if __name__ == "__main__":
    print("🚀 INICIO DEL PROCESO ATHENA CON JSON EN S3")

    crear_bucket_si_no_existe()
    generar_json()
    crear_base_datos_json()
    crear_tabla_json()

    # 1️⃣ Consulta: todos los datos
    print("\n--- CONSULTA 1: TODOS LOS DATOS ---")
    res1 = ejecutar_query(f"SELECT * FROM {TABLE_JSON} LIMIT 15;")
    mostrar_resultados(res1)

    # 2️⃣ Consulta: edad > 30
    print("\n--- CONSULTA 2: edad > 30 ---")
    res2 = ejecutar_query(f"SELECT * FROM {TABLE_JSON} WHERE edad > 50 LIMIT 10;")
    mostrar_resultados(res2)

    # 3️⃣ Consulta: conteo por ciudad
    print("\n--- CONSULTA 3: Conteo por ciudad ---")
    res3 = ejecutar_query(f"SELECT ciudad, COUNT(*) as total FROM {TABLE_JSON} GROUP BY ciudad LIMIT 5;")
    mostrar_resultados(res3)

    print("\n🎉 PROCESO ATHENA CON JSON FINALIZADO")