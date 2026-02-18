from dotenv import load_dotenv
load_dotenv()

from S3_operaciones import (
    conectar_s3,
    crear_bucket,
    crear_csv_local,
    subir_objeto_intelligent_tiering,
    descargar_objeto
)

BUCKET_NAME = "mi-bucket-intelligent-tiering-123456"
ARCHIVO = "datos_it.csv"


def main():
    print("🚀 INICIO PROCESO S3 INTELLIGENT-TIERING\n")

    s3_client = conectar_s3()

    # Crear bucket
    crear_bucket(s3_client, BUCKET_NAME)

    # Crear archivo local
    crear_csv_local(ARCHIVO)

    # Subir objeto con Intelligent-Tiering
    subir_objeto_intelligent_tiering(
        s3_client,
        BUCKET_NAME,
        ARCHIVO,
        "intelligent/datos_it.csv"
    )

    # Descargar objeto
    descargar_objeto(
        s3_client,
        BUCKET_NAME,
        "intelligent/datos_it.csv",
        "datos_it_descargado.csv"
    )

    print("🎉 PROCESO INTELLIGENT-TIERING FINALIZADO")


if __name__ == "__main__":
    main()
