from dotenv import load_dotenv
load_dotenv()

from S3_operaciones import (
    conectar_s3,
    crear_bucket,
    crear_csv_local,
    subir_csv_a_carpetas,
    descargar_objeto
)

# CONFIGURACIÓN
BUCKET_NAME = "mi-bucket-practica-s3-123456"
ARCHIVO_CSV = "datos.csv"
CARPETAS = ["raw", "processed", "analytics"]


def main():
    print("🚀 INICIO DEL PROCESO S3\n")

    s3_client = conectar_s3()
    crear_bucket(s3_client, BUCKET_NAME)
    crear_csv_local(ARCHIVO_CSV)
    subir_csv_a_carpetas(
        s3_client,
        BUCKET_NAME,
        ARCHIVO_CSV,
        CARPETAS
    )
    descargar_objeto(
        s3_client,
        BUCKET_NAME,
        "raw/datos.csv",
        "datos_descargados.csv"
    )

    print("🎉 PROCESO FINALIZADO CORRECTAMENTE")


if __name__ == "__main__":
    main()
