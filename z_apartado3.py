
from EBS_operaciones import crear_instancia
from EFS_operaciones import crear_y_montar_efs


instancia, key_path = crear_instancia(
    nombre="EC2ConEFS",
    key_name="NuevaClav"
)

efs_id = crear_y_montar_efs(instancia, key_path)
