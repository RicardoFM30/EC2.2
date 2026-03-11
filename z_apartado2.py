import os
import subprocess
import getpass
from time import sleep
from aws_session import get_ec2_resource, get_ec2_client
from botocore.exceptions import ClientError

ec2 = get_ec2_resource()
client = get_ec2_client()


def crear_key_pair(key_name):
    """Crea la KeyPair si no existe, usa la existente si el .pem está presente, y ajusta permisos en Windows"""
    key_path = os.path.join(os.getcwd(), f"{key_name}.pem")

    try:
        # Intentar crear KeyPair
        key_pair = client.create_key_pair(KeyName=key_name)

        with open(key_path, "w") as f:
            f.write(key_pair["KeyMaterial"])

        print(f"Key Pair creado y guardado en {key_path}")

    except ClientError as e:
        if "InvalidKeyPair.Duplicate" in str(e):
            print(f"KeyPair {key_name} ya existe en AWS")
            if os.path.exists(key_path):
                print(f"Usando clave local existente: {key_path}")
            else:
                raise Exception(
                    f"La KeyPair '{key_name}' existe en AWS pero el archivo {key_path} no está en tu PC.\n"
                    f"AWS no permite volver a descargarla. Debes eliminarla en AWS o usar otra."
                )
        else:
            raise e

    # Ajustar permisos según sistema operativo
    if os.name == "nt":  # Windows
        usuario = getpass.getuser()
        subprocess.run(["icacls", key_path, "/inheritance:r"], shell=True)
        subprocess.run(["icacls", key_path, "/grant:r", f"{usuario}:R"], shell=True)
    else:
        # Linux / Mac
        os.chmod(key_path, 0o400)

    return key_path


def crear_security_group(sg_nombre="MiSGSSH"):
    vpcs = list(ec2.vpcs.filter(Filters=[{"Name": "isDefault", "Values": ["true"]}]))
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
            sg_list = list(ec2.security_groups.filter(
                Filters=[{"Name": "group-name", "Values": [sg_nombre]}]
            ))
            sg = sg_list[0]
            print(f"Usando Security Group existente: {sg.id}")
        else:
            raise e

    return sg.id


def crear_instancia(nombre, key_name):
    key_path = crear_key_pair(key_name)
    sg_id = crear_security_group()

    vpcs = list(ec2.vpcs.filter(Filters=[{"Name": "isDefault", "Values": ["true"]}]))
    vpc_id = vpcs[0].id

    subnets = list(ec2.subnets.filter(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]))
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

    waiter = client.get_waiter('instance_running')
    waiter.wait(InstanceIds=[instancia.id])

    instancia.reload()
    print(f"Instancia {instancia.id} corriendo")

    return instancia, key_path


def crear_y_montar_ebs(instancia, key_file,
                       volumen_nombre="MiVolumenEBS",
                       size_gb=8,
                       device="/dev/xvdf",
                       mount_point="/mnt/volumen"):

    az = instancia.placement['AvailabilityZone']

    # Crear volumen
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

    # Esperar a que el volumen esté disponible
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

    print("Esperando a que SSH esté disponible...")
    sleep(30)

    public_ip = instancia.public_ip_address
    if not public_ip:
        print("La instancia no tiene IP pública asignada.")
        return volumen_id

    # Comandos como una sola línea, separados por &&
    comandos = (
        f"sudo mkfs -t ext4 {device} && "
        f"sudo mkdir -p {mount_point} && "
        f"sudo mount {device} {mount_point} && "
        f"echo 'Hola, EBS!' | sudo tee {mount_point}/archivo.txt"
    )

    # Comando SSH en una sola cadena
    ssh_command = f'ssh -o StrictHostKeyChecking=no -i "{key_file}" ec2-user@{public_ip} "{comandos}"'

    print("Ejecutando comandos vía SSH...")

    result = subprocess.run(ssh_command, shell=True)

    if result.returncode == 0:
        print("Volumen formateado, montado y archivo creado correctamente.")
    else:
        print("Error ejecutando comandos SSH.")

    return volumen_id

if __name__ == "__main__":

    instancia, key_path = crear_instancia(
        nombre="EBS_Ejercicio2",
        key_name="ClaveEBS_ejer2"
    )

    volumen_id = crear_y_montar_ebs(
        instancia,
        key_file=key_path
    )