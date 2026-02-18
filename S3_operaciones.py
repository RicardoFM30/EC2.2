import boto3
import os
import csv
import time


def conectar_s3():
    print("🔹 [1/6] Conectando con AWS S3...")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("ACCESS_KEY"),
        aws_secret_access_key=os.getenv("SECRET_KEY"),
        aws_session_token=os.getenv("SESSION_TOKEN"),
        region_name=os.getenv("REGION", "us-east-1")
    )
    print("✅ Conexión con S3 establecida\n")
    return s3_client


def crear_bucket(s3_client, bucket_name):
    print(f"🔹 [2/6] Creando bucket '{bucket_name}'...")
    region = os.getenv("REGION", "us-east-1")

    if region == "us-east-1":
        s3_client.create_bucket(Bucket=bucket_name)
    else:
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                "LocationConstraint": region
            }
        )

    print(f"✅ Bucket '{bucket_name}' creado correctamente\n")


def crear_csv_local(nombre_archivo):
    print(f"🔹 [3/6] Creando archivo CSV local '{nombre_archivo}'...")
    with open(nombre_archivo, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["id", "nombre", "edad"])
        writer.writerow([1, "Ana", 30])
        writer.writerow([2, "Luis", 25])
        writer.writerow([3, "Carlos", 40])
    print("✅ Archivo CSV creado\n")


def subir_csv_a_carpetas(s3_client, bucket_name, archivo_csv, carpetas):
    print("🔹 [4/6] Subiendo CSV a carpetas de S3...")
    for carpeta in carpetas:
        print(f"   ⤴ Subiendo a '{carpeta}/'...")
        s3_client.upload_file(
            Filename=archivo_csv,
            Bucket=bucket_name,
            Key=f"{carpeta}/{archivo_csv}"
        )
    print("✅ Archivos subidos correctamente\n")


def descargar_objeto(s3_client, bucket_name, key_s3, nombre_local):
    print(f"🔹 [5/6] Descargando objeto '{key_s3}'...")
    s3_client.download_file(
        Bucket=bucket_name,
        Key=key_s3,
        Filename=nombre_local
    )
    print(f"✅ Archivo descargado como '{nombre_local}'\n")

def subir_objeto_standard_ia(s3_client, bucket_name, archivo_local, key_s3):
    print("🔹 [3/5] Subiendo objeto con clase STANDARD_IA...")

    s3_client.upload_file(
        Filename=archivo_local,
        Bucket=bucket_name,
        Key=key_s3,
        ExtraArgs={
            "StorageClass": "STANDARD_IA"
        }
    )

    print("✅ Objeto subido en clase STANDARD_IA\n")


def subir_objeto_intelligent_tiering(s3_client, bucket_name, archivo_local, key_s3):
    print("🔹 [3/5] Subiendo objeto con clase INTELLIGENT_TIERING...")

    s3_client.upload_file(
        Filename=archivo_local,
        Bucket=bucket_name,
        Key=key_s3,
        ExtraArgs={
            "StorageClass": "INTELLIGENT_TIERING"
        }
    )

    print("✅ Objeto subido en clase INTELLIGENT_TIERING\n")

def subir_objeto_glacier(s3_client, bucket_name, archivo_local, key_s3):
    print("🔹 [3/6] Subiendo objeto con clase GLACIER...")

    s3_client.upload_file(
        Filename=archivo_local,
        Bucket=bucket_name,
        Key=key_s3,
        ExtraArgs={
            "StorageClass": "GLACIER"
        }
    )

    print("✅ Objeto subido en GLACIER\n")

def restaurar_objeto_glacier(s3_client, bucket_name, key_s3):
    print("🔹 [4/6] Solicitando restauración del objeto Glacier...")

    s3_client.restore_object(
        Bucket=bucket_name,
        Key=key_s3,
        RestoreRequest={
            "Days": 1,
            "GlacierJobParameters": {
                "Tier": "Standard"  # puede ser Expedited | Standard | Bulk
            }
        }
    )

    print("⏳ Restauración solicitada (puede tardar minutos u horas)\n")

def comprobar_restauracion(s3_client, bucket_name, key_s3, wait_interval=60):
    """
    Comprueba el estado de restauración de un objeto Glacier y espera
    hasta que esté disponible para descargar.
    """
    print("🔹 [5/6] Comprobando estado de restauración...")

    while True:
        response = s3_client.head_object(Bucket=bucket_name, Key=key_s3)
        restore_status = response.get("Restore")

        if restore_status:
            # Cuando 'ongoing-request="false"' significa que ya está restaurado
            if 'ongoing-request="false"' in restore_status:
                print("✅ Objeto restaurado y listo para descargar\n")
                break
            else:
                print(f"⏳ Objeto en Glacier, esperando restauración... ({restore_status})")
        else:
            print("⏳ Objeto aún no tiene restauración iniciada...")

        time.sleep(wait_interval)  # espera antes de revisar de nuevo


def subir_objeto_deep_archive(s3_client, bucket_name, archivo_local, key_s3):
    print("🔹 [3/6] Subiendo objeto con clase DEEP_ARCHIVE...")

    s3_client.upload_file(
        Filename=archivo_local,
        Bucket=bucket_name,
        Key=key_s3,
        ExtraArgs={
            "StorageClass": "DEEP_ARCHIVE"
        }
    )

    print("✅ Objeto subido en DEEP_ARCHIVE\n")

def restaurar_objeto_deep_archive(s3_client, bucket_name, key_s3):
    print("🔹 [4/6] Solicitando restauración del objeto Deep Archive...")

    s3_client.restore_object(
        Bucket=bucket_name,
        Key=key_s3,
        RestoreRequest={
            "Days": 1,
            "GlacierJobParameters": {
                "Tier": "Standard"
            }
        }
    )

    print("⏳ Restauración solicitada (puede tardar muchas horas)\n")

def comprobar_restauracion(s3_client, bucket_name, key_s3):
    print("🔹 [5/6] Comprobando estado de restauración...")

    response = s3_client.head_object(
        Bucket=bucket_name,
        Key=key_s3
    )

    restore_status = response.get("Restore", "No restaurado")

    print(f"📊 Estado: {restore_status}\n")

def activar_versionado(s3_client, bucket_name):
    print("🔹 [2/6] Activando versionado en el bucket...")

    s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={
            "Status": "Enabled"
        }
    )

    print("✅ Versionado activado\n")

def subir_objeto_con_version(s3_client, bucket_name, archivo_local, key_s3):
    print(f"🔹 Subiendo objeto '{key_s3}'...")

    response = s3_client.put_object(
        Bucket=bucket_name,
        Key=key_s3,
        Body=open(archivo_local, "rb")
    )

    version_id = response.get("VersionId")
    print(f"✅ Subido con VersionId: {version_id}\n")

    return version_id

def listar_versiones(s3_client, bucket_name, key_s3):
    print("🔹 [5/6] Listando versiones del objeto...")

    response = s3_client.list_object_versions(
        Bucket=bucket_name,
        Prefix=key_s3
    )

    versions = response.get("Versions", [])

    for v in versions:
        print(f"VersionId: {v['VersionId']} | Última: {v['IsLatest']}")

    print("✅ Versiones listadas\n")
