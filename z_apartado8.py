from dotenv import load_dotenv
load_dotenv()

from S3_operaciones import (
    conectar_s3,
    crear_bucket,
    crear_csv_local,
    subir_objeto_deep_archive,
    restaurar_objeto_deep_archive,
    comprobar_restauracion,
    descargar_objeto
)

BUCKET_NAME = "mi-bucket-deep-archive-123456"
ARCHIVO = "datos_deep.csv"


def main():
    print("🚀 INICIO PROCESO S3 DEEP ARCHIVE\n")

    s3_client = conectar_s3()

    # Crear bucket
    crear_bucket(s3_client, BUCKET_NAME)

    # Crear CSV
    crear_csv_local(ARCHIVO)

    # Subir a Deep Archive
    subir_objeto_deep_archive(
        s3_client,
        BUCKET_NAME,
        ARCHIVO,
        "deep_archive/datos_deep.csv"
    )

    # Solicitar restauración
    restaurar_objeto_deep_archive(
        s3_client,
        BUCKET_NAME,
        "deep_archive/datos_deep.csv"
    )

    # Comprobar estado
    comprobar_restauracion(
        s3_client,
        BUCKET_NAME,
        "deep_archive/datos_deep.csv"
    )

    # ⚠️ Solo funcionará cuando esté restaurado
    descargar_objeto(
        s3_client,
        BUCKET_NAME,
        "deep_archive/datos_deep.csv",
        "datos_deep_descargado.csv"
    )

    print("🎉 PROCESO DEEP ARCHIVE FINALIZADO")


if __name__ == "__main__":
    main()
