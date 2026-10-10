"""Funciones de entrada/salida y validación del proyecto AFP V2."""

from pathlib import Path
import hashlib
import json
import re
import shutil
import subprocess
import tempfile
import unicodedata


def ejecutar(args, timeout=120, check=True):
    args = [str(x) for x in args]

    r = subprocess.run(
        args,
        capture_output=True,
        text=True,
        timeout=timeout
    )

    if check and r.returncode != 0:
        raise RuntimeError(
            f"Fallo: {' '.join(args)}\n{r.stderr[:1600]}"
        )

    return r


def existe_hdfs(path):
    r = ejecutar(
        ["hdfs", "dfs", "-test", "-e", str(path)],
        check=False
    )

    if r.returncode not in (0, 1):
        raise RuntimeError(r.stderr)

    return r.returncode == 0


hdfs_existe = existe_hdfs


def sha256_local(path):
    sha = hashlib.sha256()

    with Path(path).open("rb") as archivo:
        for bloque in iter(
            lambda: archivo.read(1024 * 1024),
            b""
        ):
            sha.update(bloque)

    return sha.hexdigest()


def sha256_hdfs(path):
    proceso = subprocess.Popen(
        ["hdfs", "dfs", "-cat", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    sha = hashlib.sha256()

    for bloque in iter(
        lambda: proceso.stdout.read(1024 * 1024),
        b""
    ):
        sha.update(bloque)

    proceso.stdout.close()
    error = proceso.stderr.read().decode(
        "utf-8", errors="replace"
    )

    if proceso.wait() != 0:
        raise RuntimeError(
            f"Error leyendo HDFS: {path}\n{error}"
        )

    return sha.hexdigest()


def subir_si_no_existe(local, destino):
    local = Path(local)

    if not local.is_file():
        raise FileNotFoundError(
            f"No existe el archivo local: {local}"
        )

    # Verificar antes de subir
    if existe_hdfs(destino):
        if sha256_local(local) == sha256_hdfs(destino):
            return "ya existia verificado"

        raise RuntimeError(
            f"Archivo diferente en HDFS: {destino}. "
            "No se sobrescribira."
        )

    carpeta_hdfs = destino.rsplit("/", 1)[0]

    ejecutar([
        "hdfs", "dfs", "-mkdir", "-p", carpeta_hdfs
    ])

    with tempfile.TemporaryDirectory(
        prefix="afp_ingesta_"
    ) as carpeta_tmp:

        temporal = Path(carpeta_tmp) / "origen.bin"
        shutil.copy2(local, temporal)

        ejecutar([
            "hdfs", "dfs", "-put",
            str(temporal), str(destino)
        ], timeout=600)

    if sha256_local(local) != sha256_hdfs(destino):
        raise RuntimeError(
            f"Error de integridad SHA-256: {destino}"
        )

    return "subido y verificado"


def slug(texto):
    s = unicodedata.normalize(
        "NFKD", str(texto)
    )

    s = "".join(
        ch for ch in s
        if not unicodedata.combining(ch)
    )

    return re.sub(
        "[^a-z0-9]+", "_", s.lower()
    ).strip("_")


def escribir_jsonl(path, registros):
    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with path.open(
        "w", encoding="utf-8"
    ) as archivo:

        for registro in registros:
            archivo.write(
                json.dumps(
                    registro,
                    ensure_ascii=False,
                    sort_keys=True
                ) + "\n"
            )


def spark_session(nombre):
    import os
    import sys
    import socket
    from pyspark.sql import SparkSession

    hadoop_home = "/opt/hadoop-3.3.6"
    hadoop_conf = f"{hadoop_home}/etc/hadoop"

    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["HADOOP_CONF_DIR"] = hadoop_conf
    os.environ["YARN_CONF_DIR"] = hadoop_conf

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Obtener la IP de la máquina virtual
    ip = socket.gethostbyname(socket.gethostname())

    if ip.startswith("127."):
        ip = "10.0.2.15"

    os.environ["SPARK_LOCAL_IP"] = ip

    print("IP Driver:", ip)

    spark = (
        SparkSession.builder
        .appName(nombre)
        .master("yarn")
        .config("spark.submit.deployMode", "client")
        .config("spark.driver.host", ip)
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.executor.instances", "1")
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", "512m")
        .config("spark.driver.memory", "1g")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    print("Spark:", spark.version)
    print("MASTER:", spark.sparkContext.master)
    print(
        "HDFS:",
        spark.sparkContext._jsc
        .hadoopConfiguration()
        .get("fs.defaultFS")
    )

    if spark.sparkContext.master != "yarn":
        raise RuntimeError("Spark no está usando YARN")

    return spark


def grabar_parquet(
    df, path, clave_unica, particion="anio"
):
    """Valida claves y escribe los datos en Parquet."""

    from pyspark.sql import functions as F

    if df.limit(1).count() == 0:
        raise ValueError(
            f"Tabla vacia: {path}"
        )

    duplicados = (
        df.groupBy(*clave_unica)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicados:
        raise ValueError(
            f"Claves duplicadas: {clave_unica}"
        )

    escritor = (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
    )

    if particion and particion in df.columns:
        escritor.partitionBy(particion).parquet(path)
    else:
        escritor.parquet(path)

    print(
        "Parquet escrito:", path,
        "| filas:", df.count()
    )