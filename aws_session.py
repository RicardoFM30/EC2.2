import os
import boto3
from dotenv import load_dotenv

load_dotenv()


def _build_session():
    return boto3.session.Session(
        aws_access_key_id=os.getenv("ACCESS_KEY"),
        aws_secret_access_key=os.getenv("SECRET_KEY"),
        aws_session_token=os.getenv("SESSION_TOKEN"),
        region_name=os.getenv("REGION", "us-east-1")
    )


def get_client(service_name):
    """Devuelve un cliente boto3 para el servicio solicitado usando .env o credenciales del entorno."""
    session = _build_session()
    return session.client(service_name)


def get_resource(service_name):
    """Devuelve un resource boto3 para el servicio solicitado."""
    session = _build_session()
    return session.resource(service_name)


def get_ec2_client():
    return get_client("ec2")


def get_ec2_resource():
    return get_resource("ec2")


def get_s3_client():
    return get_client("s3")


def get_efs_client():
    return get_client("efs")


def get_athena_client():
    return get_client("athena")


def get_ssm_client():
    return get_client("ssm")
