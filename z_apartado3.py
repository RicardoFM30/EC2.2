#!/usr/bin/env python3
import os
from EC2_operaciones import crear_instancia  # tu módulo EC2
from EFS_operaciones import crear_y_montar_efs

# -----------------------------
# Parámetros
# -----------------------------
NOMBRE_INSTANCIA = "EC2ConEFS2"
KEY_NAME = "NuevaClav2"          # nombre de tu Key Pair
NOMBRE_EFS = "MiEFS2"            # nombre del EFS
MOUNT_POINT = "/mnt/efs"        # donde se montará EFS en EC2

# -----------------------------
# 1️⃣ Crear instancia EC2
# -----------------------------
print("Creando instancia EC2...")
instancia, key_path = crear_instancia(nombre=NOMBRE_INSTANCIA, key_name=KEY_NAME)

# -----------------------------
# 2️⃣ Crear y montar EFS
# -----------------------------
print("Creando y montando EFS en la instancia...")
efs_id = crear_y_montar_efs(instancia, key_path, nombre_efs=NOMBRE_EFS, mount_point=MOUNT_POINT)

print(f"\n✅ Todo listo!")
print(f"Instancia: {instancia.id}")
print(f"EFS montado: {efs_id}")
print(f"Archivo 'archivo.txt' debería estar en {MOUNT_POINT} dentro de la instancia.")
