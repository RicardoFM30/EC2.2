import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()


def conectarseEC2Cliente():

    sesion = boto3.session.Session(
        aws_access_key_id=os.getenv("ACCESS_KEY"),
        aws_secret_access_key=os.getenv("SECRET_KEY"),
        aws_session_token=os.getenv("SESSION_TOKEN"),  # opcional si usas STS
        region_name=os.getenv("REGION")
    )

    cliente_ec2 = sesion.client('ec2')
    return cliente_ec2


def conectarseEC2Recurso():

    sesion = boto3.session.Session(
        aws_access_key_id=os.getenv("ACCESS_KEY"),
        aws_secret_access_key=os.getenv("SECRET_KEY"),
        aws_session_token=os.getenv("SESSION_TOKEN"),  # opcional si usas STS
        region_name=os.getenv("REGION")
    )

    recurso_ec2 = sesion.resource('ec2')
    return recurso_ec2
