from dotenv import load_dotenv
load_dotenv()

from S3_operaciones import (
    conectar_s3,
    crear_bucket,
    crear_csv_local,
    subir_objeto_standard_ia,
    descargar_objeto
)

BUCKET_NAME = "mi-bucket-ia-practica-987654"
ARCHIVO = "datos_ia.csv"


def main():
    print("🚀 INICIO PROCESO S3 STANDARD_IA\n")

    s3_client = conectar_s3()

    crear_bucket(s3_client, BUCKET_NAME)

    crear_csv_local(ARCHIVO)

    subir_objeto_standard_ia(
        s3_client,
        BUCKET_NAME,
        ARCHIVO,
        "archivo_ia/datos_ia.csv"
    )

    # Descargar objeto
    descargar_objeto(
        s3_client,
        BUCKET_NAME,
        "archivo_ia/datos_ia.csv",
        "datos_ia_descargado.csv"
    )

    print("🎉 PROCESO STANDARD_IA FINALIZADO")


if __name__ == "__main__":
    main()
