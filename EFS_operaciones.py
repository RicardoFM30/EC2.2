import boto3
from botocore.exceptions import ClientError
from time import sleep
import os

from connectEC2 import conectarseEC2Recurso, conectarseEC2Cliente

# Conexiones
ec2 = conectarseEC2Recurso()
client = conectarseEC2Cliente()
efs_client = boto3.client("efs", region_name="us-east-1")

# --------------------------------------------------
# Crear EFS
# --------------------------------------------------
def crear_efs(nombre="MiEFS"):
    """
    Crea un sistema de archivos EFS
    Devuelve el FileSystemId
    """
    resp = efs_client.create_file_system(
        CreationToken=nombre,
        PerformanceMode="generalPurpose",
        ThroughputMode="bursting",
        Tags=[{"Key": "Name", "Value": nombre}]
    )

    efs_id = resp["FileSystemId"]
    print(f"EFS creado: {efs_id}")

    # Esperar a que esté disponible
    waiter = efs_client.get_waiter("file_system_available")
    waiter.wait(FileSystemId=efs_id)

    print(f"EFS {efs_id} disponible")
    return efs_id


# --------------------------------------------------
# Crear Mount Target
# --------------------------------------------------
def crear_mount_target(efs_id, instancia):
    """
    Crea el Mount Target en la misma subnet que la EC2
    """
    subnet_id = instancia.subnet_id
    vpc_id = instancia.vpc_id

    # Security Group de la instancia (reutilizamos)
    sg_id = instancia.security_groups[0]["GroupId"]

    try:
        mt = efs_client.create_mount_target(
            FileSystemId=efs_id,
            SubnetId=subnet_id,
            SecurityGroups=[sg_id]
        )
        print(f"Mount Target creado: {mt['MountTargetId']}")
    except ClientError as e:
        if "MountTargetConflict" in str(e):
            print("Mount Target ya existe")
        else:
            raise e

    # Esperar a que esté disponible
    sleep(20)


# --------------------------------------------------
# Montar EFS en EC2 y crear archivo
# --------------------------------------------------
def montar_efs_en_ec2(instancia, efs_id, key_file,
                     mount_point="/mnt/efs"):
    """
    Monta el EFS en la EC2 vía SSH y crea un archivo
    """

    public_ip = instancia.public_ip_address
    if not public_ip:
        raise Exception("La instancia no tiene IP pública")

    dns_efs = f"{efs_id}.efs.{client.meta.region_name}.amazonaws.com"

    comandos = f"""
sudo yum install -y amazon-efs-utils ||
sudo yum install -y nfs-utils

sudo mkdir -p {mount_point}
sudo mount -t efs {dns_efs}:/ {mount_point}
echo 'Hola desde EFS' | sudo tee {mount_point}/archivo_efs.txt
"""

    cmd_ssh = f'ssh -o StrictHostKeyChecking=no -i "{key_file}" ec2-user@{public_ip} "{comandos}"'

    print("Montando EFS y creando archivo vía SSH...")
    os.system(cmd_ssh)

    print(f"EFS montado en {mount_point}")
    print(f"Archivo creado: {mount_point}/archivo_efs.txt")


# --------------------------------------------------
# FUNCIÓN COMPLETA (todo en uno)
# --------------------------------------------------
def crear_y_montar_efs(instancia, key_file, nombre_efs="MiEFS"):
    """
    Flujo completo:
    - Crear EFS
    - Crear Mount Target
    - Montar en EC2
    - Crear archivo
    """
    efs_id = crear_efs(nombre_efs)
    crear_mount_target(efs_id, instancia)
    montar_efs_en_ec2(instancia, efs_id, key_file)
    return efs_id