import os
from dotenv import load_dotenv
from aws_session import get_s3_client
import csv
import time

# ---------------- CARGAR CREDENCIALES ---------------- #
load_dotenv()  # lee el archivo .env

# ---------------- CLIENTE S3 ---------------- #
s3_client = get_s3_client()

# ---------------- CONFIGURACIÓN DEL SCRIPT ---------------- #
BUCKET_NAME = "mi-bucket-glacier-123456"  # cambia a tu bucket
KEY_S3 = "glacier/datos_glacier.csv"
ARCHIVO_LOCAL = "datos_glacier.csv"
WAIT_INTERVAL = 60  # segundos entre comprobaciones

# ---------------- FUNCIONES ---------------- #

def crear_csv_local():
    print("🔹 [3/6] Creando archivo CSV local...")
    datos = [
        ["id", "nombre", "edad"],
        [1, "Ana", 30],
        [2, "Luis", 25],
        [3, "Carlos", 40],
        [4, "Laura", 28]
    ]
    with open(ARCHIVO_LOCAL, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(datos)
    print("✅ Archivo CSV creado\n")

def subir_objeto_glacier():
    print("🔹 [3/6] Subiendo objeto con clase GLACIER...")
    s3_client.upload_file(
        Filename=ARCHIVO_LOCAL,
        Bucket=BUCKET_NAME,
        Key=KEY_S3,
        ExtraArgs={"StorageClass": "GLACIER"}
    )
    print("✅ Objeto subido en GLACIER\n")

def restaurar_objeto_glacier():
    print("🔹 [4/6] Solicitando restauración del objeto Glacier...")
    s3_client.restore_object(
        Bucket=BUCKET_NAME,
        Key=KEY_S3,
        RestoreRequest={
            "Days": 1,
            "GlacierJobParameters": {"Tier": "Standard"}  # Expedited | Standard | Bulk
        }
    )
    print("⏳ Restauración solicitada (puede tardar minutos u horas)\n")

def esperar_restauracion():
    print("🔹 [5/6] Esperando a que el objeto esté restaurado...")
    while True:
        obj = s3_client.head_object(Bucket=BUCKET_NAME, Key=KEY_S3)
        restore = obj.get("Restore")
        if restore:
            if 'ongoing-request="false"' in restore:
                print("✅ Objeto restaurado y listo para descargar\n")
                break
            else:
                print(f"⏳ Restauración en curso... ({restore})")
        else:
            print("⏳ Restauración no iniciada todavía...")
        time.sleep(WAIT_INTERVAL)

def descargar_objeto():
    print("🔹 [6/6] Descargando objeto...")
    s3_client.download_file(BUCKET_NAME, KEY_S3, "datos_glacier_descargado.csv")
    print("✅ Descarga completada: 'datos_glacier_descargado.csv'\n")

# ---------------- MAIN ---------------- #

def main():
    print("🚀 INICIO PROCESO S3 GLACIER CON .env\n")

    print("🔹 [1/6] Conectando con AWS S3...")
    print("✅ Conexión con S3 establecida\n")

    print(f"🔹 [2/6] Creando bucket '{BUCKET_NAME}' si no existe...")
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    print(f"✅ Bucket '{BUCKET_NAME}' creado correctamente\n")

    crear_csv_local()
    subir_objeto_glacier()
    restaurar_objeto_glacier()
    esperar_restauracion()
    descargar_objeto()

    print("🎉 PROCESO S3 GLACIER FINALIZADO")

if __name__ == "__main__":
    main()
