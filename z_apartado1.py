
from EC2_operaciones import *


mi_instancia = crear_instancia("EC2_Ejercicio1")

parar_instancia(mi_instancia)

ejecutar_instancia(mi_instancia)

eliminar_instancia(mi_instancia)