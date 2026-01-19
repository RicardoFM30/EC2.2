import time
from connectEC2 import conectarseEC2Recurso
from connectEC2 import conectarseEC2Recurso, conectarseEC2Cliente
from botocore.exceptions import ClientError

ec2 = conectarseEC2Recurso()
client = conectarseEC2Cliente()  # Client: Key Pair


def crear_instancia(nombre="MiInstanciaEC2", key_name="MiKeyPair"):
    """Crea una instancia Amazon Linux 2023, con Key Pair y Security Group para SSH"""

    # Crear Key Pair si no existe
    try:
        key_pair = client.create_key_pair(KeyName=key_name)
        # Guardar la clave privada en un archivo .pem
        with open(f"{key_name}.pem", "w") as f:
            f.write(key_pair['KeyMaterial'])
        print(f"Key Pair creado y guardado en {key_name}.pem")
    except ClientError as e:
        if "InvalidKeyPair.Duplicate" in str(e):
            print(f"Usando Key Pair existente: {key_name}")
        else:
            print("Error creando Key Pair:", e)
            return

    # VPC por defecto
    vpcs = list(ec2.vpcs.filter(Filters=[{"Name":"isDefault","Values":["true"]}]))
    if not vpcs:
        print("No se encontró VPC por defecto")
        return
    vpc_id = vpcs[0].id

    # Crear o usar Security Group
    sg_nombre = "MiSGSSH"
    try:
        sg = ec2.create_security_group(
            GroupName=sg_nombre,
            Description="Security Group para SSH",
            VpcId=vpc_id
        )
        # Abrir puerto 22 (SSH)
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
            print("Error creando Security Group:", e)
            return

    # 5️⃣ Subnet por defecto
    subnets = list(ec2.subnets.filter(Filters=[{"Name":"vpc-id","Values":[vpc_id]}]))
    subnet_id = subnets[0].id

    # 6️⃣ Crear instancia con Key Pair y Security Group
    instancia = ec2.create_instances(
        ImageId="ami-07ff62358b87c7116",  # Amazon Linux 2023 x86
        InstanceType="t2.micro",
        MinCount=1,
        MaxCount=1,
        KeyName=key_name,
        SubnetId=subnet_id,
        SecurityGroupIds=[sg.id],
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": [{"Key": "Name", "Value": nombre}]
        }]
    )[0]

    print(f"Instancia creada: {instancia.id}")
    instancia.wait_until_running()
    instancia.reload()
    print(f"Instancia {instancia.id} corriendo")
    print(f"Conéctate vía SSH usando la clave: {key_name}.pem")
    
    return instancia



def parar_instancia(instancia):
    instancia.reload()  # sincroniza el estado actual
    estado = instancia.state["Name"]
    if estado == "running":
        print(f"Deteniendo instancia {instancia.id}...")
        instancia.stop()
        instancia.wait_until_stopped()
        print(f"Instancia {instancia.id} detenida")
    else:
        print(f"La instancia {instancia.id} ya está en estado '{estado}'")


def ejecutar_instancia(instancia):
    estado = instancia.state["Name"]
    if estado == "stopped":
        print(f"Iniciando instancia {instancia.id}...")
        instancia.start()
        instancia.wait_until_running()
        print(f"Instancia {instancia.id} ahora está corriendo")
    else:
        print(f"La instancia {instancia.id} ya está en estado '{estado}'")


def eliminar_instancia(instancia):
    print(f"Eliminando instancia {instancia.id}...")
    instancia.terminate()
    instancia.wait_until_terminated()
    print(f"Instancia {instancia.id} eliminada")



