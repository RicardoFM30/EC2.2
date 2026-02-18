from dotenv import load_dotenv
load_dotenv()

from S3_operaciones import (
    conectar_s3,
    crear_bucket,
    activar_versionado,
    crear_csv_local,
    subir_objeto_con_version,
    listar_versiones
)

BUCKET_NAME = "mi-bucket-versionado-123456"
ARCHIVO = "datos_versionado.csv"
KEY = "versionado/datos.csv"


def modificar_csv(nombre_archivo):
    """
    Modifica el CSV para generar una nueva versión
    """
    with open(nombre_archivo, mode="a") as file:
        file.write("\n4,Laura,28")


def main():
    print("🚀 INICIO PROCESO VERSIONADO S3\n")

    s3_client = conectar_s3()

    # Crear bucket
    crear_bucket(s3_client, BUCKET_NAME)

    # Activar versionado
    activar_versionado(s3_client, BUCKET_NAME)

    # Crear CSV inicial
    crear_csv_local(ARCHIVO)

    # Subir primera versión
    print("📌 Subiendo primera versión...")
    v1 = subir_objeto_con_version(
        s3_client,
        BUCKET_NAME,
        ARCHIVO,
        KEY
    )

    # Modificar archivo
    modificar_csv(ARCHIVO)

    # Subir segunda versión
    print("📌 Subiendo segunda versión...")
    v2 = subir_objeto_con_version(
        s3_client,
        BUCKET_NAME,
        ARCHIVO,
        KEY
    )

    # Listar versiones
    listar_versiones(
        s3_client,
        BUCKET_NAME,
        KEY
    )

    print("🎉 PROCESO VERSIONADO FINALIZADO")


if __name__ == "__main__":
    main()
