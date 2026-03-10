# Proyecto: Prácticas AWS con Python + Boto3

Este repositorio contiene scripts en Python que demuestran la creación y uso de varios servicios de almacenamiento de AWS (EC2, EBS, EFS, S3, Glacier y consultas con Athena). Los scripts usan Boto3 y están organizados por apartado.

## Estructura y apartados implementados
- EC2 (crear / iniciar / parar / eliminar): [EC2_operaciones.py](EC2_operaciones.py)
- EBS (crear / adjuntar / escribir archivo): [EBS_operaciones.py](EBS_operaciones.py)
- EFS (crear / mount target / montar y escribir): [EFS_operaciones.py](EFS_operaciones.py)
- S3 Standard (crear bucket, carpetas y subir CSVs): [S3_operaciones.py](S3_operaciones.py) y scripts `z_apartado*.py`
- S3 Standard - Acceso poco frecuente (STANDARD_IA): funciones en [S3_operaciones.py](S3_operaciones.py)
- S3 Intelligent-Tiering: funciones en [S3_operaciones.py](S3_operaciones.py)
- S3 Glacier / Deep Archive: scripts y funciones en [S3_operaciones.py](S3_operaciones.py) y `z_apartado*.py`
- Versionado de S3 (activar y mostrar dos versiones): funciones en [S3_operaciones.py](S3_operaciones.py) y ejemplo en `z_apartado11.py`
- Consultas con AWS Athena (CSV en S3): script Athena con creación de DB/tabla y 3 consultas (archivo Athena en el repo)
- Fuente JSON en Athena + 3 consultas: script que crea JSON, lo sube a S3 y ejecuta consultas (archivo Athena JSON en el repo)
- Tabla particionada y consulta sobre partición: script de particionado y ejemplo de consulta (archivo en repo)

> Nota: varios ejemplos están agrupados en los scripts `z_apartado1.py` … `z_apartado12.py` y en los módulos principales listados arriba.

## Requisitos
- Python 3.8+ (recomendado)
- Dependencias: revisar `requirements.txt` (instalar con `pip install -r requirements.txt`)
- Archivo de variables de entorno `.env` con las credenciales o usar roles/profiles de AWS:

```
ACCESS_KEY=...
SECRET_KEY=...
SESSION_TOKEN=...
REGION=us-east-1
```

## Uso rápido
1. Crear/activar entorno virtual (opcional):

```bash
python -m venv .venv
source .venv/bin/activate   # WSL / macOS / Linux
.venv\Scripts\Activate.ps1 # Windows PowerShell
pip install -r requirements.txt
```

2. Configurar `.env` o usar credenciales configuradas en tu máquina.

3. Ejecutar un ejemplo S3 (por ejemplo):

```bash
python z_apartado3.py
```

4. Otros ejemplos:
- `python z_apartado1.py` — EC2
- `python z_apartado2.py` — EBS
- `python z_apartado4.py` — EFS
- `python z_apartado5.py` / `z_apartado6.py` — clases de almacenamiento S3

## Buenas prácticas y recomendaciones
- No usar claves de acceso fijas en producción; preferir IAM roles, perfiles o AWS SSO.
- Evitar abrir SSH a `0.0.0.0/0`; usar SSM Session Manager para comandos remotos.
- Sustituir `os.system('ssh ...')` por SSM (`boto3 ssm.send_command`) o `paramiko` si es necesario.
- Añadir manejo de excepciones y `logging` en lugar de `print` para mayor trazabilidad.
- Parametrizar AMIs, regiones y tamaños de instancia en `.env` o argumentos CLI, en lugar de hardcodear.
- Añadir límites/tiempos de espera y backoff cuando se hacen polling (p.ej. restauración Glacier).

## Estado actual
- La mayoría de apartados del readme original están implementados como scripts y módulos en este repo. El código ya incluye ejemplos funcionales pero puede beneficiarse de mejoras en seguridad, manejo de errores y parametrización.

## Siguientes pasos sugeridos
- Revisar e implementar mejoras de seguridad (SSM, roles IAM).
- Reemplazar las llamadas `os.system` por SSM (requiere activar el agente SSM en instancias).
- Añadir un pequeño `USAGE.md` si quieres que deje pasos detallados para cada `z_apartadoX.py`.

Si quieres, aplico ahora alguno de los cambios sugeridos (por ejemplo: reemplazar `open()` por `with open(...)` en `S3_operaciones.py`, o añadir logging básico). 
