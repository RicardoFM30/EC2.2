import os
import subprocess
import getpass
import time
from aws_session import get_ec2_resource, get_ec2_client, get_efs_client
from botocore.exceptions import ClientError

# -----------------------------
# Recursos AWS
# -----------------------------
ec2 = get_ec2_resource()
client = get_ec2_client()
efs_client = get_efs_client()

# -----------------------------
# Key Pair
# -----------------------------
def crear_key_pair(key_name="MiKeyPair"):
    key_path = os.path.join(os.getcwd(), f"{key_name}.pem")

    try:
        key_pair = client.create_key_pair(KeyName=key_name)
        with open(key_path, "w") as f:
            f.write(key_pair['KeyMaterial'])

        # Linux: solo propietario puede leer
        if os.name != 'nt':
            os.chmod(key_path, 0o400)
        else:
            # Windows: quitar herencia y dar solo lectura al usuario
            usuario = getpass.getuser()
            subprocess.run(f'icacls "{key_path}" /inheritance:r /grant:r {usuario}:R', shell=True, check=True)

        print(f"Key Pair creado y guardado en {key_path}")

    except ClientError as e:
        if "InvalidKeyPair.Duplicate" in str(e):
            print(f"KeyPair {key_name} ya existe en AWS")
            if os.path.exists(key_path):
                print(f"Usando clave local existente: {key_path}")
            else:
                raise Exception(
                    f"La KeyPair '{key_name}' existe en AWS pero el archivo {key_path} no está en tu PC.\n"
                    "Debes eliminarla en AWS o usar otra."
                )
        else:
            raise e

    return key_path

# -----------------------------
# Security Group
# -----------------------------
def crear_security_group(sg_nombre="MiSGSSH_EFS"):
    vpcs = list(ec2.vpcs.filter(Filters=[{"Name": "isDefault", "Values": ["true"]}]))
    vpc_id = vpcs[0].id

    try:
        sg = ec2.create_security_group(
            GroupName=sg_nombre,
            Description="Security Group para SSH y EFS",
            VpcId=vpc_id
        )
        # Permitir SSH
        sg.authorize_ingress(
            IpPermissions=[{
                "IpProtocol": "tcp",
                "FromPort": 22,
                "ToPort": 22,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}]
            },
            # Permitir EFS
            {
                "IpProtocol": "tcp",
                "FromPort": 2049,
                "ToPort": 2049,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}]  # mejor: SG de EC2
            }]
        )
        print(f"Security Group creado: {sg.id}")
    except ClientError as e:
        if "InvalidGroup.Duplicate" in str(e):
            sg_list = list(ec2.security_groups.filter(Filters=[{"Name":"group-name","Values":[sg_nombre]}]))
            sg = sg_list[0]
            print(f"Usando Security Group existente: {sg.id}")
        else:
            raise e
    return sg.id

# -----------------------------
# EC2
# -----------------------------
def crear_instancia(nombre="MiInstanciaEC2", key_name="MiKeyPair"):
    key_path = crear_key_pair(key_name)
    sg_id = crear_security_group()

    vpcs = list(ec2.vpcs.filter(Filters=[{"Name":"isDefault","Values":["true"]}]))
    vpc_id = vpcs[0].id
    subnets = list(ec2.subnets.filter(Filters=[{"Name":"vpc-id","Values":[vpc_id]}]))
    subnet_id = subnets[0].id

    instancia = ec2.create_instances(
        ImageId="ami-07ff62358b87c7116",
        InstanceType="t2.micro",
        MinCount=1,
        MaxCount=1,
        KeyName=key_name,
        SubnetId=subnet_id,
        SecurityGroupIds=[sg_id],
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": [{"Key": "Name", "Value": nombre}]
        }]
    )[0]

    print(f"Instancia creada: {instancia.id}")
    client.get_waiter('instance_running').wait(InstanceIds=[instancia.id])
    instancia.reload()
    print(f"Instancia {instancia.id} corriendo")
    return instancia, key_path

# -----------------------------
# EFS
# -----------------------------
def crear_efs(nombre_efs="MiEFS"):
    for fs in efs_client.describe_file_systems().get("FileSystems", []):
        if fs.get("Name") == nombre_efs:
            print(f"Usando EFS existente: {fs['FileSystemId']}")
            return fs["FileSystemId"]

    resp = efs_client.create_file_system(
        CreationToken=nombre_efs,
        PerformanceMode="generalPurpose",
        Encrypted=False,
        Tags=[{"Key": "Name", "Value": nombre_efs}],
    )
    efs_id = resp["FileSystemId"]
    print(f"EFS creado: {efs_id}")

    while True:
        fs_desc = efs_client.describe_file_systems(FileSystemId=efs_id)["FileSystems"][0]
        if fs_desc["LifeCycleState"] == "available":
            break
        print(f"Esperando a que EFS {efs_id} esté disponible...")
        time.sleep(5)

    return efs_id

def crear_mount_target(efs_id, subnet_id, sg_id):
    for mt in efs_client.describe_mount_targets(FileSystemId=efs_id)["MountTargets"]:
        if mt["SubnetId"] == subnet_id:
            print(f"Usando Mount Target existente: {mt['MountTargetId']}")
            return mt["MountTargetId"]

    resp = efs_client.create_mount_target(
        FileSystemId=efs_id,
        SubnetId=subnet_id,
        SecurityGroups=[sg_id]
    )
    mt_id = resp["MountTargetId"]
    print(f"Mount Target creado: {mt_id}")

    while True:
        mt_desc = efs_client.describe_mount_targets(MountTargetId=mt_id)["MountTargets"][0]
        if mt_desc["LifeCycleState"] == "available":
            break
        print(f"Esperando a que Mount Target {mt_id} esté disponible...")
        time.sleep(5)

    return mt_id

# -----------------------------
# Montaje EFS
# -----------------------------
def montar_efs_ec2(instancia, efs_id, key_file, mount_point="/mnt/efs"):
    public_ip = instancia.public_ip_address
    if not public_ip:
        raise Exception("La instancia no tiene IP pública asignada")

    efs_dns = f"{efs_id}.efs.{os.getenv('REGION','us-east-1')}.amazonaws.com"

    comandos = (
        f"sudo yum install -y amazon-efs-utils && "
        f"sudo mkdir -p {mount_point} && "
        f"sudo mount -t efs -o tls {efs_dns}:/ {mount_point} && "
        f"echo 'Hola, EFS!' | sudo tee {mount_point}/archivo.txt"
    )

    ssh_command = f'ssh -o StrictHostKeyChecking=no -i "{key_file}" ec2-user@{public_ip} "{comandos}"'
    print(f"Ejecutando comandos vía SSH: {ssh_command}")
    subprocess.run(ssh_command, shell=True, check=True)
    print("EFS montado y archivo creado dentro del EFS.")

# -----------------------------
# Flujo principal
# -----------------------------
def crear_y_montar_efs(instancia, key_file, nombre_efs="MiEFS", mount_point="/mnt/efs"):
    subnet_id = instancia.subnet_id
    sg_id = instancia.security_groups[0]['GroupId']

    efs_id = crear_efs(nombre_efs)
    crear_mount_target(efs_id, subnet_id, sg_id)
    montar_efs_ec2(instancia, efs_id, key_file, mount_point)
    return efs_id

# -----------------------------
# Ejecución
# -----------------------------
if __name__ == "__main__":
    NOMBRE_INSTANCIA = "EFS_Ejercicio3"
    KEY_NAME = "ClaveEFS_ejer3"
    NOMBRE_EFS = "MiEFS2"
    MOUNT_POINT = "/mnt/efs"

    print("Creando instancia EC2...")
    instancia, key_path = crear_instancia(nombre=NOMBRE_INSTANCIA, key_name=KEY_NAME)

    print("Creando y montando EFS en la instancia...")
    efs_id = crear_y_montar_efs(instancia, key_path, nombre_efs=NOMBRE_EFS, mount_point=MOUNT_POINT)

    print(f"\n✅ Todo listo!")
    print(f"Instancia: {instancia.id}")
    print(f"EFS montado: {efs_id}")
    print(f"Archivo 'archivo.txt' debería estar en {MOUNT_POINT} dentro de la instancia.")