# Proyecto: Prácticas AWS con Python + Boto3

Este repositorio contiene scripts en Python que demuestran la creación y uso de varios servicios de almacenamiento de AWS (EC2, EBS, EFS, S3, Glacier y consultas con Athena). Los scripts usan Boto3 y están organizados por apartado.

## Estructura y apartados implementados
- Scripts por apartado: `z_apartado1.py` … `z_apartado12.py` — ejemplos autónomos para EC2, EBS, EFS, S3, Glacier y consultas con Athena.
- Operaciones S3: [S3_operaciones.py](S3_operaciones.py) — utilidades para crear buckets, subir/descargar objetos, versionado y clases de almacenamiento.
- Gestión de sesiones AWS: [aws_session.py](aws_session.py) — utilidades para crear clients/resources boto3 centralizados (reemplaza el antiguo `connectEC2.py`).

Detalles principales implementados (mapa rápido):
- EC2 (crear / iniciar / parar / eliminar): ejemplo en `z_apartado1.py`
- EBS (crear / adjuntar / escribir archivo): ejemplo en `z_apartado2.py`
- EFS (crear / mount target / montar y escribir): ejemplo en `z_apartado3.py`
- S3 Standard (crear bucket, carpetas y subir CSVs): `S3_operaciones.py` y `z_apartado3.py`/`z_apartado4.py`
- S3 Standard - Acceso poco frecuente (STANDARD_IA) e Intelligent-Tiering: funciones en `S3_operaciones.py` y ejemplos `z_apartado5.py` / `z_apartado6.py`
- S3 Glacier / Deep Archive: ejemplos en `z_apartado7.py` / `z_apartado10.py` y funciones en `S3_operaciones.py`
- Versionado de S3 (activar y mostrar dos versiones): ejemplo en `z_apartado11.py` junto a utilidades en `S3_operaciones.py`
- Consultas con AWS Athena (CSV y JSON en S3): `z_apartado10.py`, `z_apartado11.py`, `z_apartado12.py` (tablas CSV, JSON y particionadas con 3 consultas de ejemplo)

## Requisitos
- Python 3.8+
- Dependencias: revisar `requirements.txt` (instalar con `pip install -r requirements.txt`)
-- Archivo de variables de entorno `.env` con las credenciales o usar roles/perfiles de AWS:

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

2. Configurar `.env` o usar credenciales configuradas en tu máquina (perfil, role o variables de entorno).

3. Ejecutar un ejemplo S3 (por ejemplo):

```bash
python z_apartado3.py
```

4. Otros ejemplos:
- `python z_apartado1.py` — EC2
- `python z_apartado2.py` — EBS
- `python z_apartado3.py` — EFS + montaje
- `python z_apartado4.py` — S3 (flujo estándar)
- `python z_apartado5.py` / `z_apartado6.py` — clases de almacenamiento S3 (STANDARD_IA, INTELLIGENT_TIERING)


## Estado: práctica completada
Todos los ejercicios indicados en la descripción se han implementado y probado (scripts `z_apartado1.py` — `z_apartado12.py`). A continuación el listado de apartados realizado:

- Crear una instancia EC2, ejecutarla, pararla y eliminarla — `z_apartado1.py`
- Crear un EBS, asociarlo a un EC2 y añadir un archivo — `z_apartado2.py`
- Crear un EFS, montarlo y añadir un archivo — `z_apartado3.py`
- Crear S3 Estándar: crear bucket, añadir varias carpetas y subir un CSV; obtener el objeto — `z_apartado4.py` / `S3_operaciones.py`
- Crear S3 Estándar - Acceso poco frecuente (STANDARD_IA): subir y obtener objeto — `z_apartado5.py` / `S3_operaciones.py`
- Crear S3 Intelligent-Tiering: subir y obtener objeto — `z_apartado6.py` / `S3_operaciones.py`
- Crear S3 Glacier: subir objeto, solicitar restauración y comprobarla — `z_apartado7.py` / `S3_operaciones.py`
- Crear S3 Glacier Deep Archive: subir, solicitar restauración y comprobarla — `z_apartado8.py` / `S3_operaciones.py`
- Habilitar control de versiones en S3 y mostrar ejemplo con dos versiones de un objeto — `z_apartado11.py` / `S3_operaciones.py`
- Realizar 3 consultas diferentes sobre el objeto .csv del S3 usando AWS Athena — `z_apartado10.py` (genera CSV con Faker, crea DB/tabla y ejecuta 3 consultas)
- Crear otra base de datos usando una fuente JSON y aplicar 3 queries — `z_apartado11.py` (JSON example + Athena helpers)
- Crear una tabla usando una columna particionada y realizar al menos una query sobre ella — `z_apartado12.py` (tablas particionadas y `MSCK REPAIR TABLE`)

Todos los scripts incluyen mensajes por consola y comprobaciones básicas (existencia de buckets, espera de consultas Athena, manejo básico de restauraciones Glacier/Deep Archive).

