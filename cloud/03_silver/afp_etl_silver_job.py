"""AWS Glue (Spark) — ETL staging/bronze -> silver (Parquet) + calidad + catálogo.
Port de src/afp_etl.py y src/afp_integracion.py del hito parcial: misma lógica,
solo cambian las rutas (hdfs:// -> s3://) y el catálogo (Hive/Derby -> Glue Data Catalog).

Parámetros: --BRONZE <bucket> --SILVER <bucket> [--DATABASE afp_silver]
Prueba local: spark-submit afp_etl_silver_job.py --LOCAL_BRONZE dir --LOCAL_SILVER dir
"""
import csv, io, json, re, sys
from pathlib import Path


# ------------------------------------------------------------ rutas y E/S
def args():
    a = {}
    for i, x in enumerate(sys.argv):
        if x.startswith("--") and i + 1 < len(sys.argv):
            a[x[2:]] = sys.argv[i + 1]
    return a


A = args()
LOCAL = "LOCAL_BRONZE" in A
if LOCAL:
    BRONZE = "file://" + str(Path(A["LOCAL_BRONZE"]).resolve())
    SILVER = "file://" + str(Path(A["LOCAL_SILVER"]).resolve())
else:
    BRONZE = "s3://" + A["BRONZE"]
    SILVER = "s3://" + A["SILVER"]
DATABASE = A.get("DATABASE", "afp_silver")


def leer_bytes(uri, rango=None):
    """Lee un objeto pequeño (manifiesto, registro, cabecera de CSV)."""
    if uri.startswith("file://"):
        data = Path(uri[7:]).read_bytes()
        return data[:rango] if rango else data
    import boto3
    bucket, key = uri[5:].split("/", 1)
    kw = {"Bucket": bucket, "Key": key}
    if rango: kw["Range"] = f"bytes=0-{rango - 1}"
    return boto3.client("s3").get_object(**kw)["Body"].read()


def ruta_silver(zona, *componentes):
    return "/".join([SILVER, zona, *[str(x).strip("/") for x in componentes]])


def norm(uri):
    """s3://, s3a://, file:/ y file:/// apuntan al mismo objeto."""
    return re.sub(r"^[a-z0-9]+:/+", "", uri)


_MAN = None
def obtener_manifest():
    global _MAN
    if _MAN is None:
        if LOCAL:
            p = sorted(Path(BRONZE[7:], "_manifests").glob("01_manifiesto_ingesta_*.jsonl"))
            man_uri = "file://" + str(p[-1])
        else:
            import boto3
            r = boto3.client("s3").list_objects_v2(Bucket=A["BRONZE"], Prefix="_manifests/")
            man_uri = BRONZE + "/" + sorted(o["Key"] for o in r["Contents"])[-1]
        _MAN = [json.loads(l) for l in leer_bytes(man_uri).decode("utf-8").splitlines() if l.strip()]
        for x in _MAN:
            x["ruta"] = BRONZE + "/" + x["s3_key"]
    return _MAN


def registro_staging():
    return json.loads(leer_bytes(ruta_silver("staging", "_registro", "03_staging_registro.json")).decode("utf-8"))


def grabar_parquet(df, path, clave_unica, particion="anio"):
    """Igual que afp_tools.grabar_parquet: valida claves y escribe Parquet snappy."""
    from pyspark.sql import functions as F
    if df.limit(1).count() == 0:
        raise ValueError(f"Tabla vacia: {path}")
    if df.groupBy(*clave_unica).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"Claves duplicadas: {clave_unica}")
    escritor = df.write.mode("overwrite").option("compression", "snappy")
    if particion and particion in df.columns:
        escritor.partitionBy(particion).parquet(path)
    else:
        escritor.parquet(path)
    print("Parquet escrito:", path, "| filas:", df.count())


# ------------------------------------------------------------ afp_etl.py
def spark_fecha_es(col):
    """Ene95, Ene.2015 -> primer día del mes con expresiones Spark (sin UDF)."""
    from pyspark.sql import functions as F
    a = F.lower(F.regexp_replace(F.trim(col), r'[^a-zA-Z0-9]', ''))
    m = F.substring(a, 1, 3)
    meses = {'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6, 'jul': 7, 'ago': 8,
             'sep': 9, 'set': 9, 'oct': 10, 'nov': 11, 'dic': 12}
    mes = F.element_at(F.create_map(*sum(([F.lit(k), F.lit(v)] for k, v in meses.items()), [])), m)
    texto_anio = F.substring(a, 4, 4)
    anio = F.when(F.length(texto_anio) == 2,
                  F.when(texto_anio.cast('int') >= 90, texto_anio.cast('int') + 1900)
                  .otherwise(texto_anio.cast('int') + 2000)).otherwise(texto_anio.cast('int'))
    return F.to_date(F.format_string('%04d-%02d-01', anio, mes))


def bcrp(spark):
    from pyspark.sql import functions as F, Window
    man = obtener_manifest()
    csvs = [x for x in man if x['origen_relativo'].startswith('Datos Estructurados/')
            and x['origen_relativo'].lower().endswith('.csv')]
    if len(csvs) != 4: raise RuntimeError(f'Se esperan 4 CSV estructurados del BCRP; hay {len(csvs)}')
    frames = []
    for x in csvs:
        cabecera = leer_bytes(x['ruta'], 262144)
        try:
            texto = cabecera.decode('utf-8-sig'); encoding = 'UTF-8'
        except UnicodeDecodeError:
            texto = cabecera.decode('latin-1'); encoding = 'ISO-8859-1'
        lector = csv.reader(io.StringIO(texto, newline=''))
        metadata = [next(lector) for _ in range(2)]
        codigos = metadata[0]; descripciones = metadata[1]
        cols = []
        for i, cod in enumerate(codigos[1:], 1):
            if not re.fullmatch(r'PN\d{5}[A-Z]{2}', cod or ''):
                raise ValueError('Código de BCRP inesperado: ' + str(cod))
            cols.append(F.struct(F.lit(cod).alias('codigo'),
                                 F.lit(descripciones[i] if len(descripciones) > i else '').alias('descripcion'),
                                 F.col('_c' + str(i)).alias('valor_texto')))
        origin = spark.read.option('header', False).option('encoding', encoding).csv(x['ruta'])
        df = (origin.select(F.col('_c0').alias('fecha_raw'), F.explode(F.array(*cols)).alias('v'))
              .select('fecha_raw', 'v.*')
              .withColumn('fecha_mes', spark_fecha_es(F.col('fecha_raw')))
              .withColumn('valor', F.when(F.trim(F.col('valor_texto')).isin('n.d.', '', 'N.D.'), None)
                          .otherwise(F.regexp_replace(F.col('valor_texto'), ',', '.').cast('double')))
              .filter(F.col('fecha_mes').isNotNull())
              .withColumn('archivo_origen', F.lit(x['origen_relativo'])))
        frames.append(df)
    tabla = frames[0]
    for frame in frames[1:]: tabla = tabla.unionByName(frame)
    # Solo eliminar ceros iniciales de relleno, no todos los ceros financieros.
    codigos_serie = [f'PN{i:05d}MM' for i in list(range(1174, 1178)) + list(range(1179, 1183))]
    w = Window.partitionBy('codigo')
    tabla = (tabla.withColumn('primer_no_cero', F.min(F.when((F.col('valor') != 0) & F.col('valor').isNotNull(), F.col('fecha_mes'))).over(w))
             .withColumn('valor', F.when(F.col('codigo').isin(*codigos_serie) &
                                         (F.col('fecha_mes') < F.col('primer_no_cero')) & (F.col('valor') == 0), None)
                         .otherwise(F.col('valor')))
             .filter(F.col('valor').isNotNull())
             .select('codigo', 'fecha_mes', 'valor', 'descripcion', 'archivo_origen')
             .withColumn('anio', F.year('fecha_mes')))
    grabar_parquet(tabla, ruta_silver('processed', 'bcrp_series_largas'), ['codigo', 'fecha_mes'])

    jsons = [x for x in man if x['origen_relativo'].lower().endswith('.json') and
             'Datos Semiestructurados' in x['origen_relativo']]
    if len(jsons) != 5: raise RuntimeError('Deben existir exactamente 5 JSON de rentabilidad del BCRP')
    bloques = []
    for x in jsons:
        cod = re.search(r'PN\d{5}MM', x['origen_relativo'])
        if not cod: raise ValueError('JSON sin código oficial: ' + x['origen_relativo'])
        root = spark.read.option('multiLine', True).json(x['ruta'])
        parte = (root.select(F.explode('periods').alias('p'))
                 .select(F.col('p.name').alias('fecha_raw'), F.col('p.values')[0].alias('valor_texto'))
                 .withColumn('fecha_mes', spark_fecha_es(F.col('fecha_raw')))
                 .withColumn('valor', F.col('valor_texto').cast('double'))
                 .withColumn('codigo', F.lit(cod.group()))
                 .withColumn('archivo_origen', F.lit(x['origen_relativo']))
                 .filter(F.col('fecha_mes').isNotNull() & F.col('valor').isNotNull())
                 .select('codigo', 'fecha_mes', 'valor', 'archivo_origen'))
        bloques.append(parte)
    j = bloques[0]
    for frame in bloques[1:]: j = j.unionByName(frame)
    j = j.withColumn('anio', F.year('fecha_mes'))
    grabar_parquet(j, ruta_silver('processed', 'bcrp_json_rentabilidad'), ['codigo', 'fecha_mes'])
    j.groupBy('codigo').count().orderBy('codigo').show(truncate=False)
    falta = j.filter((F.col('codigo') == 'PN01178MM') & (F.col('fecha_mes') == F.lit('2018-12-01').cast('date'))).count() == 0
    print('Diciembre 2018 ausente en JSON Sistema:', falta, '-> se reconciliará con SBS, no se interpolará')


def ruta_hoja(registro, libro_fragmento, hoja_nombre):
    v = [r for r in registro['hojas'] if libro_fragmento in r['libro'] and r['hoja_slug'] == hoja_nombre]
    if len(v) != 1: raise RuntimeError(f'Hoja esperada no encontrada: {libro_fragmento}/{hoja_nombre}')
    return v[0]['ruta']


def sbs(spark):
    from pyspark.sql import functions as F
    registro = registro_staging()

    def leer(libro, hoja):
        path = ruta_hoja(registro, libro, hoja)
        if LOCAL: path = "file://" + path
        return spark.read.option('header', True).option('encoding', 'UTF-8').csv(path)

    def normalizar(df, indices, nom_valor='valor'):
        cols = [F.struct(F.lit(afp).alias('afp'), F.col('c' + str(idx)).alias('texto')) for idx, afp in indices]
        return (df.select(F.to_date('c0').alias('fecha'), F.explode(F.array(*cols)).alias('v'))
                .select('fecha', 'v.*')
                .withColumn(nom_valor, F.regexp_replace('texto', ',', '.').cast('double'))
                .drop('texto')
                .filter(F.col('fecha').isNotNull() & F.col(nom_valor).isNotNull())
                .withColumn('fecha_mes', F.trunc('fecha', 'MM')))
    afp_indices = [(1, 'Habitat'), (3, 'Integra'), (4, 'Prima'), (5, 'Profuturo')]
    afiliados = normalizar(leer('afiliacion_y_traspasos', 'afil_activos'), afp_indices)
    afiliados = (afiliados.select('fecha_mes', 'afp', F.col('valor').alias('afiliados'))
                 .withColumn('anio', F.year('fecha_mes')))
    grabar_parquet(afiliados, ruta_silver('processed', 'sbs_afiliados'), ['fecha_mes', 'afp'])

    afp_rent = afp_indices + [(11, 'Sistema')]
    rend = []
    for hoja, clase in [('rentabilidad_real_f2', 'real_12m'), ('rentabilidad_nominal_f2', 'nominal_12m')]:
        t = normalizar(leer('inversiones_y_rentabilidad', hoja), afp_rent)
        rend.append(t.select('fecha_mes', 'afp', F.lit('Fondo 2').alias('fondo'),
                             F.lit(clase).alias('tipo_rentabilidad'), F.col('valor')))
    rentabilidad = rend[0].unionByName(rend[1]).withColumn('anio', F.year('fecha_mes'))
    grabar_parquet(rentabilidad, ruta_silver('processed', 'sbs_rentabilidad'),
                   ['fecha_mes', 'afp', 'fondo', 'tipo_rentabilidad'])

    cuota = leer('valores_cuota', 'valor_cuota_diario')
    grupos = []
    for fondo in range(4):
        for j, afp in enumerate(['Habitat', 'Integra', 'Prima', 'Profuturo']):
            grupos.append(F.struct(F.lit(fondo).alias('fondo'), F.lit(afp).alias('afp'),
                                   F.col('c' + str(1 + fondo * 4 + j)).alias('texto')))
    cuota = (cuota.select(F.to_date('c0').alias('fecha'), F.explode(F.array(*grupos)).alias('v'))
             .select('fecha', 'v.*')
             .withColumn('valor_cuota', F.col('texto').cast('double')).drop('texto')
             .filter(F.col('fecha').isNotNull() & F.col('valor_cuota').isNotNull() & (F.col('valor_cuota') > 0))
             .withColumn('fecha_mes', F.trunc('fecha', 'MM'))
             .withColumn('anio', F.year('fecha')))
    grabar_parquet(cuota, ruta_silver('processed', 'sbs_valor_cuota'), ['fecha', 'afp', 'fondo'])


TERMINOS = {
    'renta_variable': r'(?i)(?U)\brenta\s+variable\b',
    'exterior': r'(?i)(?U)\bexterior\b',
    'volatilidad': r'(?i)(?U)\bvolatilidad\b',
    'retiros': r'(?i)(?U)\bretiros?\b',
    'pandemia_covid': r'(?i)(?U)\b(?:covid|pandemia)\b',
    'inflacion': r'(?i)(?U)\binflaci[oó]n\b',
    'tipo_cambio': r'(?i)(?U)\btipo\s+de\s+cambio\b',
    'tasa_interes': r'(?i)(?U)\btasa\s+de\s+inter[eé]s\b',
    'alternativos': r'(?i)(?U)\balternativos?\b',
    'sostenibilidad_esg': r'(?i)(?U)\b(?:sostenibilidad|esg)\b',
    'traspasos': r'(?i)(?U)\btraspasos?\b',
    'comisiones': r'(?i)(?U)\bcomisiones?\b',
}


def memorias(spark):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType
    docs = registro_staging()["documentos"]
    if len(docs) != 48: raise RuntimeError(f"Se esperaban 48 documentos, hay {len(docs)}")
    paths = [("file://" + d["ruta"]) if LOCAL else d["ruta"] for d in docs]
    texto = spark.createDataFrame(
        spark.sparkContext.wholeTextFiles(",".join(paths), minPartitions=2).map(lambda x: (norm(x[0]), x[1])),
        schema=StructType([StructField("ruta", StringType(), False), StructField("value", StringType(), True)]))
    meta = spark.createDataFrame([
        (norm(p), d["afp"], int(d["anio_ejercicio"]), int(d["parte"]), d["disponible_desde_regla"], d["archivo_original"])
        for p, d in zip(paths, docs)],
        ["ruta", "afp", "anio_ejercicio", "parte", "disponibilidad_raw", "archivo_original"])
    print("Documentos en manifiesto:", meta.count())
    print("Documentos leídos por Spark:", texto.count())
    if texto.groupBy("ruta").count().filter(F.col("count") > 1).count():
        raise RuntimeError("Existen rutas duplicadas en los textos")
    tabla = (meta.join(texto, "ruta", "left")
             .withColumn("disponible_desde", F.to_date("disponibilidad_raw"))
             .withColumn("caracteres", F.length("value")))
    faltantes = tabla.filter(F.col("value").isNull()).count()
    if faltantes: raise RuntimeError(f"Hay {faltantes} documentos sin texto")
    for nombre, regex in TERMINOS.items():
        tabla = tabla.withColumn("n_" + nombre, F.size(F.split(F.col("value"), regex)) - 1)
        tabla = tabla.withColumn("freq_" + nombre, F.round(
            F.col("n_" + nombre) * 10000.0 / F.greatest(F.col("caracteres"), F.lit(1)), 4))
    cols = ["afp", "anio_ejercicio", "parte", "disponible_desde", "archivo_original", "caracteres"]
    cols += ["n_" + x for x in TERMINOS] + ["freq_" + x for x in TERMINOS]
    tabla = tabla.select(*cols).withColumn("anio", F.col("anio_ejercicio"))
    print("Filas finales:", tabla.count())
    grabar_parquet(tabla, ruta_silver("processed", "memorias_indicadores"),
                   ["afp", "anio_ejercicio", "parte", "archivo_original"], particion="anio")
    tabla.groupBy("afp").count().show()


# ------------------------------------------------------------ cuaderno 14 (ampliación SBS)
HOJAS_OBJETIVO = {
    "Mov Fondo x Traspasos-Ingresos": ("mov_fondo_ingresos", "miles_soles"),
    "Mov Fondo x Traspasos-Salidas": ("mov_fondo_salidas", "miles_soles"),
    "Mov Fondo x Traspasos-Neto": ("mov_fondo_neto", "miles_soles"),
    "Traspasos Afiliados-Ingresos": ("traspasos_afiliados_ingresos", "afiliados"),
    "Traspasos Afiliados-Salidas": ("traspasos_afiliados_salidas", "afiliados"),
    "Traspasos Afiliados-Neto": ("traspasos_afiliados_neto", "afiliados"),
    "Fondo-Cartera-Encaje": ("fondo_cartera_encaje", "millones_soles"),
    "Cartera por moneda": ("cartera_por_moneda", "miles_soles"),
}


def sbs_adicionales(spark):
    """Misma lógica del cuaderno 14: pandas lee los Excel originales de bronze
    (tipos nativos de fecha y número) y Spark escribe Parquet particionado."""
    import datetime as dt, math, numbers
    import pandas as pd
    from pyspark.sql import types as T

    def convertir_fecha(valor):
        if isinstance(valor, (pd.Timestamp, dt.datetime, dt.date)):
            try:
                fecha = pd.Timestamp(valor)
                if 1990 <= fecha.year <= 2030:
                    return fecha.date()
            except (ValueError, TypeError):
                return None
        return None

    def convertir_numero(valor):
        if isinstance(valor, bool) or pd.isna(valor):
            return None
        if isinstance(valor, numbers.Number):
            numero = float(valor)
            return numero if math.isfinite(numero) else None
        return None

    def obtener_etiqueta(datos, fila_inicial, columna):
        etiquetas = []
        for i in range(max(0, fila_inicial - 4), fila_inicial):
            valor = datos.iat[i, columna]
            if pd.notna(valor):
                texto = str(valor).strip()
                if texto and not isinstance(valor, (dt.datetime, dt.date)):
                    etiquetas.append(texto)
        return " | ".join(etiquetas[-2:]) if etiquetas else f"columna_{columna}"

    registros, resumen = [], []
    for x in [m for m in obtener_manifest() if m['s3_key'].startswith('sbs/')]:
        data = leer_bytes(x['ruta'])
        motor = "openpyxl" if data[:2] == b"PK" else "xlrd"
        nombre_archivo = x['origen_relativo'].rsplit('/', 1)[-1]
        libro = pd.ExcelFile(io.BytesIO(data), engine=motor)
        for hoja_real in libro.sheet_names:
            if hoja_real.strip() not in HOJAS_OBJETIVO:
                continue
            codigo, unidad = HOJAS_OBJETIVO[hoja_real.strip()]
            datos = pd.read_excel(libro, sheet_name=hoja_real, header=None, dtype=object)
            candidatos = []
            for columna in datos.columns:
                cantidad = int(datos[columna].map(convertir_fecha).notna().sum())
                if cantidad >= 12:
                    candidatos.append((columna, cantidad))
            if not candidatos:
                raise RuntimeError("SIN COLUMNA DE FECHA: " + hoja_real)
            columna_fecha = max(candidatos, key=lambda c: c[1])[0]
            fechas = datos[columna_fecha].map(convertir_fecha)
            primera_fila = fechas.first_valid_index()
            inicio = len(registros)
            for f in range(primera_fila, len(datos)):
                fecha = fechas.iloc[f]
                if fecha is None:
                    continue
                for c in range(len(datos.columns)):
                    if c == columna_fecha:
                        continue
                    valor = convertir_numero(datos.iat[f, c])
                    if valor is None:
                        continue
                    registros.append((fecha, codigo, unidad, obtener_etiqueta(datos, primera_fila, c), valor,
                                      nombre_archivo, hoja_real.strip(), f + 1, c + 1, fecha.year))
            resumen.append((hoja_real.strip(), int(columna_fecha + 1), len(registros) - inicio))
            print(codigo, "| columna fecha:", columna_fecha + 1, "| registros:", len(registros) - inicio)
    print("TOTAL DE OBSERVACIONES ADICIONALES:", len(registros))
    if len(resumen) != 8:
        raise RuntimeError(f"Se esperaban 8 hojas objetivo; hay {len(resumen)}")
    esquema = T.StructType([
        T.StructField("fecha", T.DateType(), False), T.StructField("indicador", T.StringType(), False),
        T.StructField("unidad", T.StringType(), True), T.StructField("etiqueta_original", T.StringType(), True),
        T.StructField("valor", T.DoubleType(), False), T.StructField("archivo_origen", T.StringType(), False),
        T.StructField("hoja_origen", T.StringType(), False), T.StructField("fila_excel", T.IntegerType(), False),
        T.StructField("columna_excel", T.IntegerType(), False), T.StructField("anio", T.IntegerType(), False)])
    df = spark.createDataFrame(registros, schema=esquema)
    grabar_parquet(df.repartition(1), ruta_silver('processed', 'sbs_indicadores_adicionales'),
                   ['archivo_origen', 'hoja_origen', 'fila_excel', 'columna_excel'])
    df.groupBy("indicador").count().orderBy("indicador").show(truncate=False)


# ------------------------------------------------------------ afp_integracion.py
def construir_tabla(spark):
    from pyspark.sql import functions as F, Window
    leer = lambda t: spark.read.parquet(ruta_silver('processed', t))
    j = leer('bcrp_json_rentabilidad')
    rent_sbs = leer('sbs_rentabilidad').filter(F.col('tipo_rentabilidad') == 'real_12m')
    macros = leer('bcrp_series_largas')
    afiliados = leer('sbs_afiliados')
    cuotas = leer('sbs_valor_cuota')
    textos = leer('memorias_indicadores')

    mapa = [('PN01178MM', 'Sistema'), ('PN01179MM', 'Habitat'), ('PN01180MM', 'Integra'),
            ('PN01181MM', 'Prima'), ('PN01182MM', 'Profuturo')]
    exp = F.create_map(*sum(([F.lit(c), F.lit(n)] for c, n in mapa), []))
    nombres = ['Sistema', 'Habitat', 'Integra', 'Prima', 'Profuturo']
    wide = (j.withColumn('afp', exp[F.col('codigo')]).groupBy('fecha_mes')
            .pivot('afp', nombres).agg(F.first('valor')))
    for n in nombres: wide = wide.withColumnRenamed(n, 'json_' + n)
    sbs_wide = rent_sbs.groupBy('fecha_mes').pivot('afp', nombres).agg(F.first('valor'))
    for n in nombres: sbs_wide = sbs_wide.withColumnRenamed(n, 'sbs_' + n)
    wide = wide.join(sbs_wide, 'fecha_mes', 'left')
    # Diciembre 2018: la exportación BCRP desplaza la serie; ese mes se toma completo de SBS.
    es_dic2018 = F.col('fecha_mes') == F.to_date(F.lit('2018-12-01'))
    for n in nombres:
        wide = wide.withColumn(n, F.when(es_dic2018, F.col('sbs_' + n))
                               .otherwise(F.coalesce(F.col('json_' + n), F.col('sbs_' + n))))
    wide = wide.withColumn('fuente_sistema',
                           F.when(es_dic2018, F.lit('SBS_XLS_CORRECCION_2018_12'))
                           .when(F.col('json_Sistema').isNotNull(), F.lit('BCRP_JSON'))
                           .when(F.col('sbs_Sistema').isNotNull(), F.lit('SBS_XLS_FALLBACK'))
                           .otherwise(F.lit('SIN_DATO')))
    wide = wide.select('fecha_mes', *nombres, 'fuente_sistema')
    wide.filter(F.col('fecha_mes') == '2018-12-01').show(truncate=False)
    indicadores = (macros.filter(F.col('codigo').isin('PN01271PM', 'PN01210PM'))
                   .groupBy('fecha_mes').pivot('codigo', ['PN01271PM', 'PN01210PM']).agg(F.first('valor'))
                   .withColumnRenamed('PN01271PM', 'ipc_var_mensual_pct')
                   .withColumnRenamed('PN01210PM', 'tipo_cambio_soles_usd'))
    afi = afiliados.filter(F.col('afp') == 'Integra').select('fecha_mes', F.col('afiliados').alias('afiliados_integra'))
    mensual_cuota = (cuotas.filter((F.col('afp') == 'Integra') & (F.col('fondo') == 2))
                     .groupBy('fecha_mes').agg(F.max(F.struct('fecha', 'valor_cuota')).alias('ultimo'))
                     .select('fecha_mes', F.col('ultimo.valor_cuota').alias('valor_cuota_fin_mes_integra_f2')))

    conteos = [x.removeprefix('n_') for x in textos.columns if x.startswith('n_')]
    totales = (textos.groupBy('afp', 'anio_ejercicio')
               .agg(F.sum('caracteres').alias('caracteres_memoria'),
                    F.max('disponible_desde').alias('disponible_desde'),
                    *[F.sum('n_' + x).alias('n_' + x) for x in conteos]))
    for x in conteos:
        totales = totales.withColumn('freq_' + x,
                                     F.round(F.col('n_' + x) * 10000.0 / F.greatest('caracteres_memoria', F.lit(1)), 4))
    texto_integra = totales.filter(F.col('afp') == 'Integra').drop('afp')

    fechas = spark.sql("SELECT explode(sequence(to_date('1993-01-01'),to_date('2026-08-01'),interval 1 month)) AS fecha_mes")
    base = (fechas.join(wide, 'fecha_mes', 'left').join(indicadores, 'fecha_mes', 'left')
            .join(afi, 'fecha_mes', 'left').join(mensual_cuota, 'fecha_mes', 'left')
            .withColumn('prom_otras3', (F.col('Habitat') + F.col('Prima') + F.col('Profuturo')) / 3)
            .withColumn('brecha_real_pp', F.col('Integra') - F.col('prom_otras3')))
    w = Window.orderBy('fecha_mes')
    base = (base.withColumn('brecha_lag1', F.lag('brecha_real_pp', 1).over(w))
            .withColumn('brecha_lag3', F.lag('brecha_real_pp', 3).over(w))
            .withColumn('sistema_lag1', F.lag('Sistema', 1).over(w))
            .withColumn('ipc_lag1', F.lag('ipc_var_mensual_pct', 1).over(w))
            .withColumn('tipo_cambio_lag1', F.lag('tipo_cambio_soles_usd', 1).over(w))
            .withColumn('var_tc_12m_pct_lag1',
                        100 * (F.lag('tipo_cambio_soles_usd', 1).over(w) / F.lag('tipo_cambio_soles_usd', 13).over(w) - 1)))
    # As-of join: cada mes usa solo el último documento ya disponible.
    candidatos = (base.select('fecha_mes')
                  .join(F.broadcast(texto_integra), F.col('disponible_desde') <= F.col('fecha_mes'), 'left'))
    w_doc = Window.partitionBy('fecha_mes').orderBy(F.col('disponible_desde').desc_nulls_last())
    doc_mes = candidatos.withColumn('_orden', F.row_number().over(w_doc)).filter(F.col('_orden') == 1).drop('_orden')
    base = base.join(doc_mes, 'fecha_mes', 'left')
    base = (base.filter(F.col('fecha_mes').between('2015-01-01', '2026-08-01'))
            .withColumn('brecha_mes_siguiente_pp', F.lead('brecha_real_pp', 1).over(w))
            .withColumn('objetivo_bajo_rendimiento_mes_siguiente',
                        F.when(F.col('brecha_mes_siguiente_pp').isNull(), F.lit(None).cast('int'))
                        .when(F.col('brecha_mes_siguiente_pp') < 0, F.lit(1)).otherwise(F.lit(0)))
            .withColumn('anio', F.year('fecha_mes')))
    renombres = {'Sistema': 'rent_real_sistema_12m', 'Habitat': 'rent_real_habitat_12m',
                 'Integra': 'rent_real_integra_12m', 'Prima': 'rent_real_prima_12m',
                 'Profuturo': 'rent_real_profuturo_12m'}
    for k, v in renombres.items(): base = base.withColumnRenamed(k, v)
    grabar_parquet(base, ruta_silver('processed', 'tabla_analitica_mensual'), ['fecha_mes'], particion=None)
    nulos = base.select(*[F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in (
        'rent_real_integra_12m', 'rent_real_habitat_12m', 'rent_real_prima_12m',
        'rent_real_profuturo_12m', 'rent_real_sistema_12m', 'ipc_lag1', 'tipo_cambio_lag1',
        'objetivo_bajo_rendimiento_mes_siguiente')])
    print('Nulos principales de tabla analítica:')
    nulos.show(truncate=False)
    print('Nota: el último mes carece de etiqueta futura; NO debe entrenarse con etiqueta inventada.')


TABLAS = {
    'bcrp_series_largas': ['codigo', 'fecha_mes'],
    'bcrp_json_rentabilidad': ['codigo', 'fecha_mes'],
    'sbs_afiliados': ['afp', 'fecha_mes'],
    'sbs_rentabilidad': ['fecha_mes', 'afp', 'fondo', 'tipo_rentabilidad'],
    'sbs_valor_cuota': ['fecha', 'afp', 'fondo'],
    'memorias_indicadores': ['afp', 'anio_ejercicio', 'parte'],
    'tabla_analitica_mensual': ['fecha_mes'],
    'sbs_indicadores_adicionales': ['archivo_origen', 'hoja_origen', 'fila_excel', 'columna_excel'],
}


def calidad_y_catalogo(spark):
    from pyspark.sql import functions as F
    esperado_por_tabla = {
        'bcrp_series_largas': {'codigo': 'string', 'fecha_mes': 'date', 'valor': 'double'},
        'bcrp_json_rentabilidad': {'codigo': 'string', 'fecha_mes': 'date', 'valor': 'double'},
        'sbs_afiliados': {'fecha_mes': 'date', 'afp': 'string', 'afiliados': 'double'},
        'sbs_rentabilidad': {'fecha_mes': 'date', 'afp': 'string', 'valor': 'double'},
        'sbs_valor_cuota': {'fecha': 'date', 'fondo': 'int', 'valor_cuota': 'double'},
        'memorias_indicadores': {'afp': 'string', 'disponible_desde': 'date', 'caracteres': 'int'},
        'tabla_analitica_mensual': {'fecha_mes': 'date', 'objetivo_bajo_rendimiento_mes_siguiente': 'int'},
        'sbs_indicadores_adicionales': {'fecha': 'date', 'indicador': 'string', 'valor': 'double'},
    }
    resumen = []
    for nombre, claves in TABLAS.items():
        df = spark.read.parquet(ruta_silver('processed', nombre))
        reales = dict(df.dtypes)
        errores_tipos = [f'{c}: esperado {t}, obtenido {reales.get(c)}'
                         for c, t in esperado_por_tabla[nombre].items() if reales.get(c) != t]
        if errores_tipos: raise RuntimeError('Esquema no válido en ' + nombre + ': ' + str(errores_tipos))
        n = df.count()
        d = df.groupBy(*claves).count().filter(F.col('count') > 1).count()
        print(f'{nombre:29} filas={n:7} duplicados_clave={d}')
        if d: raise RuntimeError('Duplicados en ' + nombre)
        resumen.append((nombre, n, d))
        df.createOrReplaceTempView('v_' + nombre)
    total = sum(r[1] for r in resumen)
    print(f'TOTAL DE FILAS ALMACENADAS: {total:,} | supera 100000: {total > 100000}')
    spark.createDataFrame(resumen, ['tabla', 'filas', 'duplicados_clave']).coalesce(1) \
        .write.mode('overwrite').json(ruta_silver('quality', 'resumen_tablas'))

    t = spark.read.parquet(ruta_silver('processed', 'tabla_analitica_mensual'))
    campos = ['rent_real_integra_12m', 'rent_real_habitat_12m', 'rent_real_prima_12m', 'rent_real_profuturo_12m',
              'rent_real_sistema_12m', 'ipc_var_mensual_pct', 'tipo_cambio_soles_usd', 'afiliados_integra',
              'valor_cuota_fin_mes_integra_f2', 'anio_ejercicio']
    detalles = [F.struct(F.lit(c).alias('campo'), F.col(c).isNull().alias('sin_correspondencia'))
                for c in campos if c in t.columns]
    faltantes = (t.select('fecha_mes', F.explode(F.array(*detalles)).alias('v'))
                 .select('fecha_mes', 'v.*').filter('sin_correspondencia'))
    faltantes.write.mode('overwrite').parquet(ruta_silver('quality', 'uniones_sin_correspondencia'))
    print('Total de campos sin correspondencia por mes (incluye cobertura SBS menor que BCRP):')
    faltantes.groupBy('campo').count().orderBy(F.desc('count')).show(truncate=False)

    # Catálogo: en Glue, enableHiveSupport + --enable-glue-datacatalog = Glue Data Catalog.
    if spark.conf.get('spark.sql.catalogImplementation', 'in-memory') == 'hive':
        spark.sql(f'CREATE DATABASE IF NOT EXISTS {DATABASE}')
        for nombre in TABLAS:
            spark.sql(f'DROP TABLE IF EXISTS {DATABASE}.{nombre}')
            spark.sql(f"CREATE TABLE {DATABASE}.{nombre} USING PARQUET LOCATION '{ruta_silver('processed', nombre)}'")
            if nombre != 'tabla_analitica_mensual':
                spark.sql(f'MSCK REPAIR TABLE {DATABASE}.{nombre}')
        print(f'Tablas registradas en el catálogo, base {DATABASE}:')
        spark.sql(f'SHOW TABLES IN {DATABASE}').show(truncate=False)
    else:
        print('Catálogo no activo (prueba local): solo vistas temporales v_*')
    spark.sql('SELECT anio, COUNT(*) AS meses FROM v_tabla_analitica_mensual GROUP BY anio ORDER BY anio').show(30)


def main():
    from pyspark.sql import SparkSession
    b = SparkSession.builder.appName('afp-03-etl-silver')
    if not LOCAL: b = b.enableHiveSupport()
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    bcrp(spark)
    sbs(spark)
    memorias(spark)
    sbs_adicionales(spark)
    construir_tabla(spark)
    calidad_y_catalogo(spark)
    print('ETL silver completado')


if __name__ == '__main__':
    main()
