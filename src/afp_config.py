"""Configuración única del proyecto AFP V2; no modifica el proyecto anterior."""
from pathlib import Path
import getpass

BASE = Path.home() / "BigData_UPAO" / "proyecto_afp_v2"
INPUTS = BASE / "inputs" / "drive"
NOTEBOOKS = BASE / "notebooks"
EVIDENCIAS = BASE / "evidencias"
STAGING_LOCAL = BASE / "salidas_locales" / "staging"
USUARIO_HDFS = getpass.getuser()  # Cambiar aquí únicamente si tu cuenta de HDFS es distinta.
HDFS = f"/user/{USUARIO_HDFS}/datalake_afp_v2"

def raiz_fuentes():
    """Detecta ZIP extraído CON o SIN carpeta envolvente 'Archivos proyecto'."""
    marcadores = {"Datos Estructurados", "Datos Semiestructurados", "Datos No estructurados", "Revisión"}
    if not INPUTS.exists():
        raise FileNotFoundError(f"No existe {INPUTS}. Primero sube los archivos del Drive")
    if marcadores.issubset({p.name for p in INPUTS.iterdir()}):
        return INPUTS
    for p in INPUTS.iterdir():
        if p.is_dir() and marcadores.issubset({q.name for q in p.iterdir()}):
            return p
    raise FileNotFoundError(f"Faltan carpetas de Drive dentro de {INPUTS}.")


def ruta_hdfs(zona, *componentes):
    return "/".join([HDFS, zona, *[str(x).strip("/") for x in componentes]])
