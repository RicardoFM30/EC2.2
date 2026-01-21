import os
import boto3
from time import sleep
from dotenv import load_dotenv
from connectEC2 import conectarseEC2Cliente, conectarseEC2Recurso

load_dotenv()

# -----------------------------
# Conexiones
# -----------------------------
ec2 = conectarseEC2Recurso()
ec2_client = conectarseEC2Cliente()

session = boto3.session.Session(
    aws_access_key_id=os.getenv("ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SECRET_KEY"),
    aws_session_token=os.getenv("SESSION_TOKEN"),
    region_name=os.getenv("REGION")
)

efs_client = session.client("efs")

# -----------------------------
# Crear EFS
# -----------------------------
def crear_efs(nombre="MiEFS"):
    resp = efs_client.create_file_system(
        CreationToken=nombre,
        Tags=[{"Key": "Name", "Value": nombre}]
    )

    efs_id = resp["FileSystemId"]
    print(f"EFS creado: {efs_id}")

    # Espera activa hasta que esté disponible
    print("Esperando a que EFS esté disponible...")
    while True:
        desc = efs_client.describe_file_systems(
            FileSystemId=efs_id
        )

        estado = desc["FileSystems"][0]["LifeCycleState"]
        print(f"Estado EFS: {estado}")

        if estado == "available":
            break

        sleep(5)

    print(f"EFS {efs_id} disponible")
    return efs_id


# -----------------------------
# Crear Mount Target
# -----------------------------
def crear_mount_target(efs_id, subnet_id, sg_id):
    resp = efs_client.create_mount_target(
        FileSystemId=efs_id,
        SubnetId=subnet_id,
        SecurityGroups=[sg_id]
    )

    mt_id = resp["MountTargetId"]
    print(f"Mount Target creado: {mt_id}")

    return mt_id

# -----------------------------
# Montar EFS en EC2 y crear archivo
# -----------------------------
def crear_y_montar_efs(instancia, key_file,
                        nombre_efs="MiEFS",
                        mount_point="/mnt/efs"):
    # 1️⃣ Crear EFS
    efs_id = crear_efs(nombre_efs)

    # 2️⃣ Obtener subnet y SG de la instancia
    instancia.reload()

    subnet_id = instancia.subnet_id
    sg_id = instancia.security_groups[0]["GroupId"]

    # 3️⃣ Crear Mount Target
    crear_mount_target(efs_id, subnet_id, sg_id)

    # 4️⃣ Esperar a que EFS esté accesible
    print("Esperando a que EFS esté listo para montar...")
    sleep(30)

    # 5️⃣ Montar vía SSH
    public_ip = instancia.public_ip_address
    if not public_ip:
        print("La instancia no tiene IP pública")
        return efs_id

    region = os.getenv("REGION")

    comandos = f"""
sudo yum install -y amazon-efs-utils ||
sudo yum install -y nfs-utils

sudo mkdir -p {mount_point}

sudo mount -t efs -o tls {efs_id}:/ {mount_point}

echo 'Hola desde EFS' | sudo tee {mount_point}/archivo_efs.txt
"""

    cmd_ssh = (
        f'ssh -o StrictHostKeyChecking=no '
        f'-i "{key_file}" ec2-user@{public_ip} "{comandos}"'
    )

    print(f"Ejecutando SSH:\n{cmd_ssh}")
    os.system(cmd_ssh)

    print("EFS montado y archivo creado correctamente")
    return efs_id
