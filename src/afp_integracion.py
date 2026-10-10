"""Integra SBS+BCRP+memorias mediante fecha_mes, afp, fondo y disponibilidad temporal."""
from afp_config import ruta_hdfs
from afp_tools import grabar_parquet


def construir_tabla(spark):
    from pyspark.sql import functions as F, Window
    leer=lambda t:spark.read.parquet(ruta_hdfs('processed',t))
    j=leer('bcrp_json_rentabilidad')
    rent_sbs=leer('sbs_rentabilidad').filter(F.col('tipo_rentabilidad')=='real_12m')
    macros=leer('bcrp_series_largas')
    afiliados=leer('sbs_afiliados')
    cuotas=leer('sbs_valor_cuota')
    textos=leer('memorias_indicadores')

    mapa=[('PN01178MM','Sistema'),('PN01179MM','Habitat'),('PN01180MM','Integra'),
          ('PN01181MM','Prima'),('PN01182MM','Profuturo')]
    exp=F.create_map(*sum(([F.lit(c),F.lit(n)] for c,n in mapa),[]))
    wide=(j.withColumn('afp',exp[F.col('codigo')])
           .groupBy('fecha_mes')
           .pivot('afp',['Sistema','Habitat','Integra','Prima','Profuturo'])
           .agg(F.first('valor')))
    nombres=['Sistema','Habitat','Integra','Prima','Profuturo']
    for n in nombres: wide=wide.withColumnRenamed(n,'json_'+n)
    sbs_wide=(rent_sbs.groupBy('fecha_mes').pivot('afp',nombres).agg(F.first('valor')))
    for n in nombres:sbs_wide=sbs_wide.withColumnRenamed(n,'sbs_'+n)
    wide=wide.join(sbs_wide,'fecha_mes','left')
    # Diciembre 2018: la exportación BCRP desplaza la serie de AFP en el CSV y JSON
    # frente al esquema SBS; usar para ESE mes todos los valores SBS de la misma fecha,
    # no combinar columnas desplazadas (reconciliación auditable).
    es_dic2018=F.col('fecha_mes')==F.to_date(F.lit('2018-12-01'))
    for n in nombres:
        wide=wide.withColumn(n,F.when(es_dic2018,F.col('sbs_'+n))
                .otherwise(F.coalesce(F.col('json_'+n),F.col('sbs_'+n))))
    wide=wide.withColumn('fuente_sistema',
        F.when(es_dic2018,F.lit('SBS_XLS_CORRECCION_2018_12'))
         .when(F.col('json_Sistema').isNotNull(),F.lit('BCRP_JSON'))
         .when(F.col('sbs_Sistema').isNotNull(),F.lit('SBS_XLS_FALLBACK'))
         .otherwise(F.lit('SIN_DATO')))
    wide=wide.select('fecha_mes',*nombres,'fuente_sistema')
    wide.filter(F.col('fecha_mes')=='2018-12-01').show(truncate=False)
    indicadores=(macros.filter(F.col('codigo').isin('PN01271PM','PN01210PM'))
                 .groupBy('fecha_mes').pivot('codigo',['PN01271PM','PN01210PM'])
                 .agg(F.first('valor'))
                 .withColumnRenamed('PN01271PM','ipc_var_mensual_pct')
                 .withColumnRenamed('PN01210PM','tipo_cambio_soles_usd'))
    afi=afiliados.filter(F.col('afp')=='Integra').select('fecha_mes',F.col('afiliados').alias('afiliados_integra'))
    mensual_cuota=(cuotas.filter((F.col('afp')=='Integra')&(F.col('fondo')==2))
                  .groupBy('fecha_mes').agg(F.max(F.struct('fecha','valor_cuota')).alias('ultimo'))
                  .select('fecha_mes',F.col('ultimo.valor_cuota').alias('valor_cuota_fin_mes_integra_f2')))

    # Del texto se agrega por ejercicio/AFP (hay años con más de un documento).
    conteos=[x.removeprefix('n_') for x in textos.columns if x.startswith('n_')]
    totales=(textos.groupBy('afp','anio_ejercicio')
            .agg(F.sum('caracteres').alias('caracteres_memoria'),
                 F.max('disponible_desde').alias('disponible_desde'),
                 *[F.sum('n_'+x).alias('n_'+x) for x in conteos]))
    for x in conteos:
        totales=totales.withColumn('freq_'+x,
                 F.round(F.col('n_'+x)*10000.0/F.greatest('caracteres_memoria',F.lit(1)),4))
    texto_integra=totales.filter(F.col('afp')=='Integra').drop('afp')

    fechas=spark.sql("SELECT explode(sequence(to_date('1993-01-01'),to_date('2026-08-01'),interval 1 month)) AS fecha_mes")
    base=(fechas.join(wide,'fecha_mes','left')
          .join(indicadores,'fecha_mes','left')
          .join(afi,'fecha_mes','left')
          .join(mensual_cuota,'fecha_mes','left')
          .withColumn('prom_otras3',(F.col('Habitat')+F.col('Prima')+F.col('Profuturo'))/3)
          .withColumn('brecha_real_pp',F.col('Integra')-F.col('prom_otras3')))
    w=Window.orderBy('fecha_mes')
    base=(base.withColumn('brecha_lag1',F.lag('brecha_real_pp',1).over(w))
          .withColumn('brecha_lag3',F.lag('brecha_real_pp',3).over(w))
          .withColumn('sistema_lag1',F.lag('Sistema',1).over(w))
          .withColumn('ipc_lag1',F.lag('ipc_var_mensual_pct',1).over(w))
          .withColumn('tipo_cambio_lag1',F.lag('tipo_cambio_soles_usd',1).over(w))
          .withColumn('var_tc_12m_pct_lag1',
              100*(F.lag('tipo_cambio_soles_usd',1).over(w)/
                   F.lag('tipo_cambio_soles_usd',13).over(w)-1)))
    # As-of join: cada mes usa solo el último documento ya disponible. Nunca toma el año 2025 en 2025.
    candidatos=(base.select('fecha_mes')
          .join(F.broadcast(texto_integra), F.col('disponible_desde')<=F.col('fecha_mes'),'left'))
    w_doc=Window.partitionBy('fecha_mes').orderBy(F.col('disponible_desde').desc_nulls_last())
    doc_mes=(candidatos.withColumn('_orden',F.row_number().over(w_doc))
             .filter(F.col('_orden')==1).drop('_orden'))
    base=base.join(doc_mes,'fecha_mes','left')
    base=(base.filter(F.col('fecha_mes').between('2015-01-01','2026-08-01'))
          .withColumn('brecha_mes_siguiente_pp',F.lead('brecha_real_pp',1).over(w))
          .withColumn('objetivo_bajo_rendimiento_mes_siguiente',
              F.when(F.col('brecha_mes_siguiente_pp').isNull(),F.lit(None).cast('int'))
               .when(F.col('brecha_mes_siguiente_pp')<0,F.lit(1)).otherwise(F.lit(0)))
          .withColumn('anio',F.year('fecha_mes')))
    renombres={'Sistema':'rent_real_sistema_12m','Habitat':'rent_real_habitat_12m',
               'Integra':'rent_real_integra_12m','Prima':'rent_real_prima_12m',
               'Profuturo':'rent_real_profuturo_12m'}
    for k,v in renombres.items():base=base.withColumnRenamed(k,v)
    path=ruta_hdfs('processed','tabla_analitica_mensual')
    grabar_parquet(base,path,['fecha_mes'],particion=None)
    nulos=base.select(*[F.count(F.when(F.col(c).isNull(),c)).alias(c) for c in (
      'rent_real_integra_12m','rent_real_habitat_12m','rent_real_prima_12m',
      'rent_real_profuturo_12m','rent_real_sistema_12m','ipc_lag1','tipo_cambio_lag1',
      'objetivo_bajo_rendimiento_mes_siguiente')])
    print('Nulos principales de tabla analítica:')
    nulos.show(truncate=False)
    print('Nota: el último mes carece de etiqueta futura; NO debe entrenarse con etiqueta inventada.')
    return base


def calidad_y_catalogo(spark):
    from pyspark.sql import functions as F
    tablas={
      'bcrp_series_largas':['codigo','fecha_mes'],
      'bcrp_json_rentabilidad':['codigo','fecha_mes'],
      'sbs_afiliados':['afp','fecha_mes'],
      'sbs_rentabilidad':['fecha_mes','afp','fondo','tipo_rentabilidad'],
      'sbs_valor_cuota':['fecha','afp','fondo'],
      'memorias_indicadores':['afp','anio_ejercicio','parte'],
      'tabla_analitica_mensual':['fecha_mes'],
    }
    for nombre,claves in tablas.items():
        df=spark.read.parquet(ruta_hdfs('processed',nombre))
        # Contratos de tipos de la zona procesada: compara esquema real frente al esperado.
        esperado={
          'bcrp_series_largas': {'codigo':'string','fecha_mes':'date','valor':'double'},
          'bcrp_json_rentabilidad': {'codigo':'string','fecha_mes':'date','valor':'double'},
          'sbs_afiliados': {'fecha_mes':'date','afp':'string','afiliados':'double'},
          'sbs_rentabilidad': {'fecha_mes':'date','afp':'string','valor':'double'},
          'sbs_valor_cuota': {'fecha':'date','fondo':'int','valor_cuota':'double'},
          'memorias_indicadores': {'afp':'string','disponible_desde':'date','caracteres':'int'},
          'tabla_analitica_mensual': {'fecha_mes':'date','objetivo_bajo_rendimiento_mes_siguiente':'int'},
        }[nombre]
        reales=dict(df.dtypes)
        errores_tipos=[f'{c}: esperado {tipo}, obtenido {reales.get(c)}'
                       for c,tipo in esperado.items() if reales.get(c)!=tipo]
        if errores_tipos:raise RuntimeError('Esquema no válido en '+nombre+': '+str(errores_tipos))
        n=df.count()
        d=df.groupBy(*claves).count().filter(F.col('count')>1).count()
        print(f'{nombre:29} filas={n:7} duplicados_clave={d}')
        if d:raise RuntimeError('Duplicados en '+nombre)
        df.createOrReplaceTempView('v_'+nombre)
    # Conciliación de uniones LEFT: diferencias reales se reportan, no se rellenan a ciegas.
    t=spark.read.parquet(ruta_hdfs('processed','tabla_analitica_mensual'))
    campos=['rent_real_integra_12m','rent_real_habitat_12m',
            'rent_real_prima_12m','rent_real_profuturo_12m',
            'rent_real_sistema_12m','ipc_var_mensual_pct',
            'tipo_cambio_soles_usd','afiliados_integra',
            'valor_cuota_fin_mes_integra_f2','anio_ejercicio']
    detalles=[F.struct(F.lit(c).alias('campo'),F.col(c).isNull().alias('sin_correspondencia'))
              for c in campos if c in t.columns]
    faltantes=(t.select('fecha_mes',F.explode(F.array(*detalles)).alias('v'))
               .select('fecha_mes','v.*').filter('sin_correspondencia'))
    faltantes.write.mode('overwrite').parquet(ruta_hdfs('quality','uniones_sin_correspondencia'))
    print('Total de campos sin correspondencia por mes (incluye cobertura SBS menor que BCRP):')
    faltantes.groupBy('campo').count().orderBy(F.desc('count')).show(truncate=False)
    implementacion=spark.conf.get('spark.sql.catalogImplementation','in-memory')
    if implementacion=='hive':
        try:
            spark.sql('CREATE DATABASE IF NOT EXISTS afp_v2')
            for nombre in tablas:
                ubicacion=ruta_hdfs('processed',nombre)
                spark.sql(f"CREATE TABLE IF NOT EXISTS afp_v2.{nombre} USING PARQUET LOCATION '{ubicacion}'")
                if nombre!='tabla_analitica_mensual':
                    spark.sql(f'MSCK REPAIR TABLE afp_v2.{nombre}')
            print('Hive conectado: tablas externas registradas en la base afp_v2')
        except Exception as e:
            print('Hive no accesible para registrar tablas:',e)
            print('Se conservan rutas Parquet y vistas TEMPORALES, no declarar Hive implementado.')
    else:
        print('Hive Metastore NO activo. Se registraron vistas TEMPORALES v_* para esta sesión.')
    print('Consulta de control Spark SQL:')
    spark.sql('SELECT anio, COUNT(*) AS meses FROM v_tabla_analitica_mensual GROUP BY anio ORDER BY anio').show()
