# Big Data - Sistema Privado de Pensiones (AFP)

Proyecto académico de análisis de datos del Sistema Privado de Pensiones del Perú.

## Arquitectura

Ruta A (on-premise):

- HDFS: almacenamiento distribuido.
- Apache YARN: gestión de recursos.
- Apache Spark 3.5.1: procesamiento de datos.
- JupyterLab: desarrollo y ejecución de notebooks.
- Hive Metastore compatible: catálogo persistente con Apache Derby.
- Parquet: almacenamiento de datos procesados.

## Fuentes

- SBS: archivos Excel de afiliación, rentabilidad y valor cuota.
- BCRP: series económicas CSV y rentabilidad JSON.
- AFP: memorias anuales PDF, DOC y DOCX.
- Archivos auxiliares de revisión y control.

Los datos originales se mantienen fuera del repositorio Git.

## Organización

- `notebooks/`: notebooks de ingesta, procesamiento y verificación.
- `src/`: módulos Python del ETL.
- `inputs/drive/`: fuentes locales, no incluidas en Git.
- `evidencias/`: resultados y registros locales.
- `metastore/`: catálogo Derby local, no incluido en Git.

## Data lake

Ruta HDFS utilizada en el laboratorio:

`/user/junior/datalake_afp_v2/`

Zonas:

- `raw/`: archivos originales.
- `staging/`: archivos preparados.
- `processed/`: tablas Parquet.
- `quality/`: controles de calidad.
- `catalog/`: recursos del catálogo.

## Resultados de referencia

Siete tablas analíticas y 140 meses integrados, desde enero de 2015 hasta agosto de 2026.

Los conteos y validaciones se encuentran documentados en los notebooks.

## Reproducción

Cada integrante debe configurar sus propias rutas de HDFS, Spark y el acceso a los archivos originales.

Las rutas absolutas del laboratorio original deben adaptarse antes de ejecutar el pipeline en otra máquina.

## Colaboración

Se recomienda trabajar en ramas individuales y enviar Pull Requests a `main`.
