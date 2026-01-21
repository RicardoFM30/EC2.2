import boto3
import os
import time

# Conexión con credenciales y región desde variables de entorno
efs_client = boto3.client(
    "efs",
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),  # opcional si usas STS
    region_name=os.getenv("REGION", "us-east-1")
)

ec2_client = boto3.client(
    "ec2",
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION", "us-east-1")
)


def crear_efs(nombre_efs="MiEFS"):
    """
    Crea un EFS o devuelve el existente
    """
    # Revisar si existe un EFS con el mismo token de creación
    try:
        respuesta = efs_client.describe_file_systems()
        for fs in respuesta.get("FileSystems", []):
            if fs.get("Name") == nombre_efs:
                print(f"Usando EFS existente: {fs['FileSystemId']}")
                return fs["FileSystemId"]
    except efs_client.exceptions.FileSystemNotFound:
        pass

    # Crear EFS
    resp = efs_client.create_file_system(
        CreationToken=nombre_efs,
        PerformanceMode="generalPurpose",
        Encrypted=False,
        Tags=[{"Key": "Name", "Value": nombre_efs}],
    )
    efs_id = resp["FileSystemId"]
    print(f"EFS creado: {efs_id}")

    # Esperar a que el EFS esté disponible
    while True:
        fs_desc = efs_client.describe_file_systems(FileSystemId=efs_id)["FileSystems"][0]
        if fs_desc["LifeCycleState"] == "available":
            break
        print(f"Esperando a que EFS {efs_id} esté disponible...")
        time.sleep(5)

    return efs_id


def crear_mount_target(efs_id, subnet_id, sg_id):
    """
    Crea Mount Target para el EFS en la subred especificada si no existe
    """
    # Revisar si ya existe Mount Target en la subred
    mts = efs_client.describe_mount_targets(FileSystemId=efs_id)["MountTargets"]
    for mt in mts:
        if mt["SubnetId"] == subnet_id:
            print(f"Usando Mount Target existente: {mt['MountTargetId']}")
            return mt["MountTargetId"]

    # Crear Mount Target
    resp = efs_client.create_mount_target(
        FileSystemId=efs_id,
        SubnetId=subnet_id,
        SecurityGroups=[sg_id]
    )
    mt_id = resp["MountTargetId"]
    print(f"Mount Target creado: {mt_id}")

    # Esperar a que esté disponible
    while True:
        mt_desc = efs_client.describe_mount_targets(MountTargetId=mt_id)["MountTargets"][0]
        if mt_desc["LifeCycleState"] == "available":
            break
        print(f"Esperando a que el Mount Target {mt_id} esté disponible...")
        time.sleep(5)

    return mt_id


def montar_efs_ec2(instancia, efs_id, key_file, mount_point="/mnt/efs"):
    """
    Monta el EFS en la instancia EC2 y crea un archivo archivo.txt
    """
    public_ip = instancia.public_ip_address
    if not public_ip:
        raise Exception("La instancia no tiene IP pública asignada")

    # Instalar cliente de EFS
    comandos = f"""
sudo yum install -y amazon-efs-utils &&
sudo mkdir -p {mount_point} &&
sudo mount -t efs {efs_id}.efs.{os.getenv('REGION', 'us-east-1')}.amazonaws.com:/ {mount_point} &&
echo 'Hola, EFS!' | sudo tee {mount_point}/archivo.txt
"""
    cmd_ssh = f'ssh -o StrictHostKeyChecking=no -i "{key_file}" ec2-user@{public_ip} "{comandos}"'
    print(f"Ejecutando comandos via SSH: {cmd_ssh}")
    os.system(cmd_ssh)

    print("EFS montado y archivo creado dentro del EFS.")


def crear_y_montar_efs(instancia, key_file, nombre_efs="MiEFS", mount_point="/mnt/efs"):
    """
    Función completa para EFS: crea EFS, Mount Target y monta en EC2
    """
    # Obtener subred y SG de la instancia
    subnets = instancia.subnet_id
    vpc_id = instancia.vpc_id
    # Tomamos el primer SG
    sg_id = instancia.security_groups[0]['GroupId']

    # Crear EFS
    efs_id = crear_efs(nombre_efs)

    # Crear Mount Target en la subred de la EC2
    crear_mount_target(efs_id, subnets, sg_id)

    # Montar EFS en EC2
    montar_efs_ec2(instancia, efs_id, key_file, mount_point)

    return efs_id
