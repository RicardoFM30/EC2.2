from EBS_operaciones import *


instancia, key_path = crear_instancia(nombre="EC2ConEBS", key_name="ClaveNueva")
volumen_id = crear_y_montar_ebs(instancia, key_file=key_path)
