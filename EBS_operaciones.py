import boto3
from botocore.exceptions import ClientError
from connectEC2 import conectarseEC2Recurso, conectarseEC2Cliente
from time import sleep
import os

# Conexiones globales
ec2 = conectarseEC2Recurso()
client = conectarseEC2Cliente()  # necesario para waiters y operaciones de cliente

# -----------------------------
# Función para crear Key Pair
# -----------------------------
def crear_key_pair(key_name="MiKeyPair"):
    """
    Crea un Key Pair y lo guarda localmente en el directorio actual.
    Devuelve la ruta absoluta al archivo .pem
    """
    key_path = os.path.join(os.getcwd(), f"{key_name}.pem")

    try:
        key_pair = client.create_key_pair(KeyName=key_name)
        with open(key_path, "w") as f:
            f.write(key_pair['KeyMaterial'])
        os.chmod(key_path, 0o400)  # permisos correctos para SSH
        print(f"Key Pair creado y guardado en {key_path}")
    except ClientError as e:
        if "InvalidKeyPair.Duplicate" in str(e):
            print(f"Usando Key Pair existente: {key_path}")
            if not os.path.exists(key_path):
                print(f"Atención: {key_path} no existe, necesitas crear uno nuevo o descargarlo")
        else:
            raise e

    return key_path

# -----------------------------
# Función para crear Security Group
# -----------------------------
def crear_security_group(sg_nombre="MiSGSSH"):
    vpcs = list(ec2.vpcs.filter(Filters=[{"Name":"isDefault","Values":["true"]}]))
    vpc_id = vpcs[0].id

    try:
        sg = ec2.create_security_group(
            GroupName=sg_nombre,
            Description="Security Group para SSH",
            VpcId=vpc_id
        )
        sg.authorize_ingress(
            IpPermissions=[{
                "IpProtocol": "tcp",
                "FromPort": 22,
                "ToPort": 22,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}]
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
# Función para crear instancia EC2
# -----------------------------
def crear_instancia(nombre="MiInstanciaEC2", key_name="MiKeyPair"):
    key_path = crear_key_pair(key_name)
    sg_id = crear_security_group()

    # Subnet por defecto
    vpcs = list(ec2.vpcs.filter(Filters=[{"Name":"isDefault","Values":["true"]}]))
    vpc_id = vpcs[0].id
    subnets = list(ec2.subnets.filter(Filters=[{"Name":"vpc-id","Values":[vpc_id]}]))
    subnet_id = subnets[0].id

    instancia = ec2.create_instances(
        ImageId="ami-07ff62358b87c7116",  # Amazon Linux 2023 x86
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

    # Waiter para que la instancia esté corriendo
    waiter = client.get_waiter('instance_running')
    waiter.wait(InstanceIds=[instancia.id])
    instancia.reload()
    print(f"Instancia {instancia.id} corriendo")

    return instancia, key_path  # devolvemos también la ruta al .pem

# -----------------------------
# Función para crear y montar EBS usando SSH
# -----------------------------
def crear_y_montar_ebs(instancia, key_file, volumen_nombre="MiVolumenEBS", size_gb=8,
                        device="/dev/sdf", mount_point="/mnt/volumen"):
    az = instancia.placement['AvailabilityZone']

    # Crear volumen EBS
    volumen_resp = client.create_volume(
        Size=size_gb,
        VolumeType='gp3',
        AvailabilityZone=az,
        TagSpecifications=[{
            "ResourceType": "volume",
            "Tags": [{"Key": "Name", "Value": volumen_nombre}]
        }]
    )
    volumen_id = volumen_resp['VolumeId']
    print(f"Volumen creado: {volumen_id}")

    # Waiter para que el volumen esté disponible
    waiter_vol = client.get_waiter('volume_available')
    waiter_vol.wait(VolumeIds=[volumen_id])
    print(f"Volumen {volumen_id} disponible")

    # Adjuntar volumen
    client.attach_volume(
        VolumeId=volumen_id,
        InstanceId=instancia.id,
        Device=device
    )
    print(f"Volumen {volumen_id} adjuntado a la instancia {instancia.id}")

    sleep(10)  # esperar a que el SO detecte el disco

    # Montar volumen vía SSH usando os.system
    public_ip = instancia.public_ip_address
    if not public_ip:
        print("La instancia no tiene IP pública asignada.")
        return volumen_id

    comandos = f"""
sudo mkfs -t ext4 {device} &&
sudo mkdir -p {mount_point} &&
sudo mount {device} {mount_point} &&
echo 'Hola, EBS!' | sudo tee {mount_point}/archivo.txt
"""
    cmd_ssh = f'ssh -o StrictHostKeyChecking=no -i "{key_file}" ec2-user@{public_ip} "{comandos}"'
    print(f"Ejecutando comandos via SSH: {cmd_ssh}")
    os.system(cmd_ssh)

    print("Volumen formateado, montado y archivo creado dentro del volumen vía SSH.")
    return volumen_id