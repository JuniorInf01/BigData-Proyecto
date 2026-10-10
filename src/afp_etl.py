"""ETL con Spark de los tres tipos de datos del proyecto.
Solo las etapas de extracción/conversión técnica se hacen antes de Spark.
"""
from pathlib import Path
import csv, json, re
from afp_config import EVIDENCIAS, raiz_fuentes, ruta_hdfs
from afp_tools import grabar_parquet, spark_session


def obtener_manifest():
    p=sorted(EVIDENCIAS.glob('01_manifiesto_ingesta_*.jsonl'))
    if not p:
        # Compatibilidad con primer kit compartido
        p=sorted(EVIDENCIAS.glob('manifiesto_ingesta_*.jsonl'))
    if not p:raise RuntimeError('Ejecuta antes notebook 01 de ingesta')
    return [json.loads(line) for line in p[-1].read_text(encoding='utf-8').splitlines() if line.strip()]


def buscar(manifest, *subcadenas):
    candidatos=[x for x in manifest if all(v.lower() in x['origen_relativo'].lower() for v in subcadenas)]
    if len(candidatos)!=1:
        raise RuntimeError(f'Se esperaba un archivo para {subcadenas}; encontrados: {len(candidatos)}')
    return candidatos[0]


def spark_fecha_es(col):
    """Ene95, Ene.2015 → primer día del mes con expresiones Spark (sin UDF)."""
    from pyspark.sql import functions as F
    a=F.lower(F.regexp_replace(F.trim(col),r'[^a-zA-Z0-9]',''))
    m=F.substring(a,1,3)
    meses={'ene':1,'feb':2,'mar':3,'abr':4,'may':5,'jun':6,'jul':7,'ago':8,
           'sep':9,'set':9,'oct':10,'nov':11,'dic':12}
    mes=F.element_at(F.create_map(*sum(([F.lit(k),F.lit(v)] for k,v in meses.items()),[])),m)
    texto_anio=F.expr('1') if False else F.substring(a,4,4)
    anio=F.when(F.length(texto_anio)==2,
                F.when(texto_anio.cast('int')>=90,texto_anio.cast('int')+1900)
                .otherwise(texto_anio.cast('int')+2000)).otherwise(texto_anio.cast('int'))
    return F.to_date(F.format_string('%04d-%02d-01',anio,mes))


def bcrp(spark):
    from pyspark.sql import functions as F, Window
    raiz=raiz_fuentes(); man=obtener_manifest()
    csvs=[x for x in man if x['origen_relativo'].startswith('Datos Estructurados/')
          and x['origen_relativo'].lower().endswith('.csv')]
    if len(csvs)!=4:raise RuntimeError(f'Se esperan 4 CSV estructurados del BCRP; hay {len(csvs)}')
    frames=[]
    for x in csvs:
        local=raiz/x['origen_relativo']
        try:
            with local.open('r',encoding='utf-8-sig',newline='') as f:
                metadata=[next(csv.reader(f)) for _ in range(2)]
            encoding='UTF-8'
        except UnicodeDecodeError:
            with local.open('r',encoding='latin-1',newline='') as f:
                metadata=[next(csv.reader(f)) for _ in range(2)]
            encoding='ISO-8859-1'
        codigos=metadata[0]
        descripciones=metadata[1]
        cols=[]
        for i,cod in enumerate(codigos[1:],1):
            if not re.fullmatch(r'PN\d{5}[A-Z]{2}',cod or ''):
                raise ValueError('Código de BCRP inesperado: '+str(cod))
            cols.append(F.struct(F.lit(cod).alias('codigo'),
                 F.lit(descripciones[i] if len(descripciones)>i else '').alias('descripcion'),
                 F.col('_c'+str(i)).alias('valor_texto')))
        origin=spark.read.option('header',False).option('encoding',encoding).csv(x['hdfs_path'])
        df=(origin.select(F.col('_c0').alias('fecha_raw'),F.explode(F.array(*cols)).alias('v'))
            .select('fecha_raw','v.*')
            .withColumn('fecha_mes',spark_fecha_es(F.col('fecha_raw')))
            .withColumn('valor', F.when(F.trim(F.col('valor_texto')).isin('n.d.','', 'N.D.'),None)
                       .otherwise(F.regexp_replace(F.col('valor_texto'),',','.').cast('double')))
            .filter(F.col('fecha_mes').isNotNull())
            .withColumn('archivo_origen',F.lit(x['origen_relativo'])))
        frames.append(df)
    tabla=frames[0]
    for frame in frames[1:]:tabla=tabla.unionByName(frame)
    # Solo eliminar ceros iniciales de relleno, no todos los ceros financieros.
    codigos_serie=[f'PN{i:05d}MM' for i in list(range(1174,1178))+list(range(1179,1183))]
    w=Window.partitionBy('codigo')
    tabla=(tabla.withColumn('primer_no_cero',F.min(F.when((F.col('valor')!=0)&F.col('valor').isNotNull(),F.col('fecha_mes'))).over(w))
          .withColumn('valor',F.when(F.col('codigo').isin(*codigos_serie)&
             (F.col('fecha_mes')<F.col('primer_no_cero'))&(F.col('valor')==0),None)
             .otherwise(F.col('valor')))
          .filter(F.col('valor').isNotNull())
          .select('codigo','fecha_mes','valor','descripcion','archivo_origen')
          .withColumn('anio',F.year('fecha_mes')))
    destino=ruta_hdfs('processed','bcrp_series_largas')
    grabar_parquet(tabla,destino,['codigo','fecha_mes'])

    # JSON BCRP semiestructurado; cada archivo contiene config + array periods.
    jsons=[x for x in man if x['origen_relativo'].lower().endswith('.json') and
           'Datos Semiestructurados' in x['origen_relativo']]
    if len(jsons)!=5:raise RuntimeError('Deben existir exactamente 5 JSON de rentabilidad del BCRP')
    bloques=[]
    for x in jsons:
        cod=re.search(r'PN\d{5}MM',x['origen_relativo'])
        if not cod: raise ValueError('JSON sin código oficial: '+x['origen_relativo'])
        root=spark.read.option('multiLine',True).json(x['hdfs_path'])
        parte=(root.select(F.explode('periods').alias('p'))
             .select(F.col('p.name').alias('fecha_raw'),F.col('p.values')[0].alias('valor_texto'))
             .withColumn('fecha_mes',spark_fecha_es(F.col('fecha_raw')))
             .withColumn('valor',F.col('valor_texto').cast('double'))
             .withColumn('codigo',F.lit(cod.group()))
             .withColumn('archivo_origen',F.lit(x['origen_relativo']))
             .filter(F.col('fecha_mes').isNotNull()&F.col('valor').isNotNull())
             .select('codigo','fecha_mes','valor','archivo_origen'))
        bloques.append(parte)
    j=bloques[0]
    for frame in bloques[1:]:j=j.unionByName(frame)
    j=j.withColumn('anio',F.year('fecha_mes'))
    grabar_parquet(j,ruta_hdfs('processed','bcrp_json_rentabilidad'),['codigo','fecha_mes'])
    j.groupBy('codigo').count().orderBy('codigo').show(truncate=False)
    falta=j.filter((F.col('codigo')=='PN01178MM')& (F.col('fecha_mes')==F.lit('2018-12-01').cast('date'))).count()==0
    print('Diciembre 2018 ausente en JSON Sistema:',falta,'→ se reconciliará con SBS, no se interpolará')
    return tabla,j


def ruta_hoja(registro,libro_fragmento,hoja_nombre):
    v=[r for r in registro['hojas'] if libro_fragmento in r['libro'] and r['hoja_slug']==hoja_nombre]
    if len(v)!=1:raise RuntimeError(f'Hoja esperada no encontrada: {libro_fragmento}/{hoja_nombre}')
    return v[0]['hdfs_path']


def sbs(spark):
    from pyspark.sql import functions as F
    registro=json.loads((EVIDENCIAS/'03_staging_registro.json').read_text(encoding='utf-8'))
    def leer(libro,hoja):
        path=ruta_hoja(registro,libro,hoja)
        return spark.read.option('header',True).option('encoding','UTF-8').csv(path)
    def normalizar(df,descripcion,indices,nom_valor='valor'):
        cols=[F.struct(F.lit(afp).alias('afp'),F.col('c'+str(idx)).alias('texto')) for idx,afp in indices]
        return (df.select(F.to_date('c0').alias('fecha'),F.explode(F.array(*cols)).alias('v'))
               .select('fecha','v.*')
               .withColumn(nom_valor,F.regexp_replace('texto',',','.').cast('double'))
               .drop('texto')
               .filter(F.col('fecha').isNotNull()&F.col(nom_valor).isNotNull())
               .withColumn('fecha_mes',F.trunc('fecha','MM')))
    afp_indices=[(1,'Habitat'),(3,'Integra'),(4,'Prima'),(5,'Profuturo')]
    afiliados=normalizar(leer('afiliacion_y_traspasos','afil_activos'),'afiliacion',afp_indices)
    afiliados=(afiliados.select('fecha_mes','afp',F.col('valor').alias('afiliados'))
               .withColumn('anio',F.year('fecha_mes')))
    grabar_parquet(afiliados,ruta_hdfs('processed','sbs_afiliados'),['fecha_mes','afp'])

    afp_rent=afp_indices+[(11,'Sistema')]
    rend=[]
    for hoja,clase in [('rentabilidad_real_f2','real_12m'),('rentabilidad_nominal_f2','nominal_12m')]:
        t=normalizar(leer('inversiones_y_rentabilidad',hoja),clase,afp_rent)
        t=t.select('fecha_mes','afp',F.lit('Fondo 2').alias('fondo'),
                   F.lit(clase).alias('tipo_rentabilidad'),F.col('valor'))
        rend.append(t)
    rentabilidad=(rend[0].unionByName(rend[1]).withColumn('anio',F.year('fecha_mes')))
    grabar_parquet(rentabilidad,ruta_hdfs('processed','sbs_rentabilidad'),
                   ['fecha_mes','afp','fondo','tipo_rentabilidad'])

    cuota=leer('valores_cuota','valor_cuota_diario')
    grupos=[]
    for fondo in range(4):
        for j,afp in enumerate(['Habitat','Integra','Prima','Profuturo']):
            grupos.append(F.struct(F.lit(fondo).alias('fondo'),F.lit(afp).alias('afp'),
                      F.col('c'+str(1+fondo*4+j)).alias('texto')))
    cuota=(cuota.select(F.to_date('c0').alias('fecha'),F.explode(F.array(*grupos)).alias('v'))
        .select('fecha','v.*')
        .withColumn('valor_cuota',F.col('texto').cast('double')).drop('texto')
        .filter(F.col('fecha').isNotNull()&F.col('valor_cuota').isNotNull()&(F.col('valor_cuota')>0))
        .withColumn('fecha_mes',F.trunc('fecha','MM'))
        .withColumn('anio',F.year('fecha')))
    grabar_parquet(cuota,ruta_hdfs('processed','sbs_valor_cuota'),['fecha','afp','fondo'])
    return afiliados,rentabilidad,cuota


TERMINOS={
    'renta_variable':r'(?i)(?U)\brenta\s+variable\b',
    'exterior':r'(?i)(?U)\bexterior\b',
    'volatilidad':r'(?i)(?U)\bvolatilidad\b',
    'retiros':r'(?i)(?U)\bretiros?\b',
    'pandemia_covid':r'(?i)(?U)\b(?:covid|pandemia)\b',
    'inflacion':r'(?i)(?U)\binflaci[oó]n\b',
    'tipo_cambio':r'(?i)(?U)\btipo\s+de\s+cambio\b',
    'tasa_interes':r'(?i)(?U)\btasa\s+de\s+inter[eé]s\b',
    'alternativos':r'(?i)(?U)\balternativos?\b',
    'sostenibilidad_esg':r'(?i)(?U)\b(?:sostenibilidad|esg)\b',
    'traspasos':r'(?i)(?U)\btraspasos?\b',
    'comisiones':r'(?i)(?U)\bcomisiones?\b',
}


def memorias(spark):
    import json
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType
    )

    registro = json.loads(
        (EVIDENCIAS / "03_staging_registro.json")
        .read_text(encoding="utf-8")
    )

    docs = registro["documentos"]

    if len(docs) != 48:
        raise RuntimeError(
            f"Se esperaban 48 documentos, hay {len(docs)}"
        )

    paths = [d["hdfs_path"] for d in docs]

    sc = spark.sparkContext

    registros_texto = sc.wholeTextFiles(
        ",".join(paths),
        minPartitions=2
    ).map(
        lambda x: (
            x[0].split("://", 1)[-1]
                 .split("/", 1)[-1]
                 .join([]) if False else
            "/" + x[0].split("://", 1)[-1]
                      .split("/", 1)[-1],
            x[1]
        )
    )

    esquema = StructType([
        StructField("ruta", StringType(), False),
        StructField("value", StringType(), True)
    ])

    texto = spark.createDataFrame(
        registros_texto,
        schema=esquema
    )

    meta = spark.createDataFrame([
        (
            d["hdfs_path"],
            d["afp"],
            int(d["anio_ejercicio"]),
            int(d["parte"]),
            d["disponible_desde_regla"],
            d["archivo_original"]
        )
        for d in docs
    ], [
        "ruta",
        "afp",
        "anio_ejercicio",
        "parte",
        "disponibilidad_raw",
        "archivo_original"
    ])

    print("Documentos en manifiesto:", meta.count())
    print("Documentos leídos por Spark:", texto.count())

    duplicados_texto = (
        texto.groupBy("ruta")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicados_texto:
        raise RuntimeError(
            "Existen rutas duplicadas en los textos"
        )

    tabla = (
        meta.join(texto, "ruta", "left")
        .withColumn(
            "disponible_desde",
            F.to_date("disponibilidad_raw")
        )
        .withColumn(
            "caracteres",
            F.length("value")
        )
    )

    faltantes = tabla.filter(
        F.col("value").isNull()
    ).count()

    if faltantes:
        raise RuntimeError(
            f"Hay {faltantes} documentos sin texto"
        )

    for nombre, regex in TERMINOS.items():

        tabla = tabla.withColumn(
            "n_" + nombre,
            F.size(
                F.split(F.col("value"), regex)
            ) - 1
        )

        tabla = tabla.withColumn(
            "freq_" + nombre,
            F.round(
                F.col("n_" + nombre) * 10000.0 /
                F.greatest(
                    F.col("caracteres"),
                    F.lit(1)
                ),
                4
            )
        )

    cols = [
        "afp",
        "anio_ejercicio",
        "parte",
        "disponible_desde",
        "archivo_original",
        "caracteres"
    ]

    cols += [
        "n_" + x for x in TERMINOS
    ]

    cols += [
        "freq_" + x for x in TERMINOS
    ]

    tabla = (
        tabla.select(*cols)
        .withColumn(
            "anio",
            F.col("anio_ejercicio")
        )
    )

    print("Filas finales:", tabla.count())

    grabar_parquet(
        tabla,
        ruta_hdfs(
            "processed",
            "memorias_indicadores"
        ),
        [
            "afp",
            "anio_ejercicio",
            "parte",
            "archivo_original"
        ],
        particion="anio"
    )

    tabla.groupBy("afp").count().show()

    return tabla
