"""AWS Glue (Spark) — Optimización (broadcast join) y prueba de escalabilidad.

Responde a las observaciones del parcial:
  1. Broadcast medido con 3 corridas y sin descartar calentamiento
     -> 3 corridas de calentamiento descartadas, 15 medidas por estrategia,
        orden intercalado (A,B,B,A...), mediana, rango intercuartil e IC 95 %
        por bootstrap de la diferencia; además a 1x, 10x y 100x.
  2. Escalabilidad que partía de Parquet ya normalizado, sin punto de quiebre
     ni memoria del executor
     -> parte del CSV de staging (texto crudo de la hoja de valor cuota),
        aplica la misma transformación del ETL (tipado, explode, filtros),
        une con la tabla mensual, agrega y escribe Parquet; escala hasta que
        el tiempo por millón de filas se degrada o el job falla; registra
        memoria pico del executor, spill a memoria/disco y shuffle desde la
        API REST de Spark.

Parámetros: --SILVER <bucket> [--ESCALAS 1,10,100,300,1000,2000,3000] [--REPS 15]
Prueba local: spark-submit afp_benchmark_job.py --LOCAL_SILVER dir --ESCALAS 1,2 --REPS 3
"""
import json, random, statistics, sys, time, urllib.request
from datetime import datetime, timezone


def args():
    a = {}
    for i, x in enumerate(sys.argv):
        if x.startswith("--") and i + 1 < len(sys.argv):
            a[x[2:]] = sys.argv[i + 1]
    return a


A = args()
LOCAL = "LOCAL_SILVER" in A
SILVER = ("file://" + A["LOCAL_SILVER"]) if LOCAL else ("s3://" + A["SILVER"])
ESCALAS = [int(x) for x in A.get("ESCALAS", "1,10,100,300,1000,2000,3000").split(",")]
REPS = int(A.get("REPS", "15"))
CALENTAMIENTO = 3
LIMITE_SEG = float(A.get("LIMITE_SEG", "900"))   # una escala que pase de 15 min se considera quiebre
SALIDA = SILVER + "/benchmark"


def p(*x):
    print(*x, flush=True)


# ------------------------------------------------------------ métricas de Spark (API REST del driver)
def rest(spark, ruta):
    url = spark.sparkContext.uiWebUrl
    if not url:
        return None
    app = spark.sparkContext.applicationId
    try:
        with urllib.request.urlopen(f"{url}/api/v1/applications/{app}/{ruta}", timeout=10) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None


def metricas_desde(spark, stage_inicial):
    """Suma spill/shuffle de las etapas nuevas y toma la memoria pico de los executors."""
    out = {"spill_memoria_mb": None, "spill_disco_mb": None, "shuffle_escrito_mb": None,
           "gc_seg": None, "heap_pico_executor_mb": None, "memoria_unificada_mb": None,
           "executors": None}
    st = rest(spark, "stages?status=complete")
    if st is not None:
        nuevas = [s for s in st if s["stageId"] >= stage_inicial]
        mb = lambda k: round(sum(s.get(k, 0) for s in nuevas) / 2**20, 1)
        picos = [(s.get("peakExecutorMetrics") or {}).get("JVMHeapMemory", 0) for s in nuevas]
        out.update(spill_memoria_mb=mb("memoryBytesSpilled"), spill_disco_mb=mb("diskBytesSpilled"),
                   shuffle_escrito_mb=mb("shuffleWriteBytes"),
                   gc_seg=round(sum(s.get("jvmGcTime", 0) for s in nuevas) / 1000, 1),
                   heap_pico_executor_mb=round(max(picos) / 2**20, 1) if picos and max(picos) else None)
    ex = rest(spark, "allexecutors")
    if ex is not None:
        ejec = [e for e in ex if e["id"] != "driver"] or ex
        out.update(memoria_unificada_mb=round(max(e.get("maxMemory", 0) for e in ejec) / 2**20, 1),
                   executors=len(ejec))
        if out["heap_pico_executor_mb"] is None:   # respaldo: pico acumulado del executor
            pk = [(e.get("peakMemoryMetrics") or {}).get("JVMHeapMemory", 0) for e in ejec]
            out["heap_pico_executor_mb"] = round(max(pk) / 2**20, 1) if pk else None
    return out


def ultimo_stage(spark):
    st = rest(spark, "stages")
    return (max(s["stageId"] for s in st) + 1) if st else 0


# ------------------------------------------------------------ 1. broadcast join
def broadcast_benchmark(spark):
    from pyspark.sql import functions as F
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    cuotas0 = spark.read.parquet(SILVER + "/processed/sbs_valor_cuota") \
        .select("fecha", "afp", "fondo", "valor_cuota", "fecha_mes")
    mensual = spark.read.parquet(SILVER + "/processed/tabla_analitica_mensual") \
        .select("fecha_mes", "rent_real_integra_12m").cache()
    mensual.count()

    def consulta(cuotas, bcast):
        der = F.broadcast(mensual) if bcast else mensual
        return (cuotas.join(der, "fecha_mes", "inner")
                .withColumn("valor_ponderado", F.col("valor_cuota") * F.col("rent_real_integra_12m"))
                .groupBy("afp", "fondo")
                .agg(F.count("*").alias("registros"), F.sum("valor_ponderado").alias("total")))

    resultados = []
    for k in [1, 10, 100]:
        cuotas = cuotas0.crossJoin(F.broadcast(spark.range(k).withColumnRenamed("id", "replica"))) \
            .drop("replica").cache()
        n = cuotas.count()
        plan = {}
        for nombre, b in [("sin_broadcast", False), ("con_broadcast", True)]:
            plan[nombre] = "BroadcastHashJoin" if "BroadcastHashJoin" in \
                consulta(cuotas, b)._jdf.queryExecution().executedPlan().toString() else "SortMergeJoin"
        import math
        ra = sorted((r.afp, r.fondo, r.registros, r.total) for r in consulta(cuotas, False).collect())
        rb = sorted((r.afp, r.fondo, r.registros, r.total) for r in consulta(cuotas, True).collect())
        assert len(ra) == len(rb) and all(x[:3] == y[:3] and math.isclose(x[3], y[3], rel_tol=1e-9)
                                          for x, y in zip(ra, rb)), "Resultados distintos con y sin broadcast"
        for _ in range(CALENTAMIENTO):           # calentamiento descartado (JIT, caché, planificador)
            consulta(cuotas, False).collect(); consulta(cuotas, True).collect()
        t = {"sin_broadcast": [], "con_broadcast": []}
        for i in range(REPS):                    # orden intercalado A,B / B,A para no sesgar por posición
            orden = [False, True] if i % 2 == 0 else [True, False]
            for b in orden:
                t0 = time.perf_counter(); consulta(cuotas, b).collect()
                t["con_broadcast" if b else "sin_broadcast"].append(time.perf_counter() - t0)
        rng = random.Random(42)
        difs = []
        for _ in range(2000):                    # bootstrap de la diferencia de medianas
            a = [rng.choice(t["sin_broadcast"]) for _ in range(REPS)]
            c = [rng.choice(t["con_broadcast"]) for _ in range(REPS)]
            difs.append(statistics.median(a) - statistics.median(c))
        difs.sort()
        q = lambda v: statistics.quantiles(v, n=4)
        fila = {"escala": k, "filas_valor_cuota": n, "repeticiones": REPS, "calentamiento_descartado": CALENTAMIENTO,
                "plan_sin": plan["sin_broadcast"], "plan_con": plan["con_broadcast"],
                "mediana_sin_s": round(statistics.median(t["sin_broadcast"]), 3),
                "mediana_con_s": round(statistics.median(t["con_broadcast"]), 3),
                "iqr_sin_s": [round(x, 3) for x in (q(t["sin_broadcast"])[0], q(t["sin_broadcast"])[2])],
                "iqr_con_s": [round(x, 3) for x in (q(t["con_broadcast"])[0], q(t["con_broadcast"])[2])],
                "ic95_dif_s": [round(difs[50], 3), round(difs[1949], 3)],
                "tiempos_sin_s": [round(x, 3) for x in t["sin_broadcast"]],
                "tiempos_con_s": [round(x, 3) for x in t["con_broadcast"]]}
        fila["mejora_pct"] = round(100 * (1 - fila["mediana_con_s"] / fila["mediana_sin_s"]), 1)
        resultados.append(fila)
        p(f"[broadcast {k}x] {n:,} filas | sin {fila['mediana_sin_s']} s ({plan['sin_broadcast']}) | "
          f"con {fila['mediana_con_s']} s ({plan['con_broadcast']}) | mejora {fila['mejora_pct']} % | "
          f"IC95 dif {fila['ic95_dif_s']} s")
        cuotas.unpersist()
    mensual.unpersist()
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
    return resultados


# ------------------------------------------------------------ 2. escalabilidad de extremo a extremo
def leer_registro_staging(spark):
    sc = spark.sparkContext
    txt = sc.wholeTextFiles(SILVER + "/staging/_registro/03_staging_registro.json").collect()[0][1]
    return json.loads(txt)


def escalabilidad(spark):
    from pyspark.sql import functions as F
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    reg = leer_registro_staging(spark)
    hoja = [h for h in reg["hojas"] if "valores_cuota" in h["libro"] and h["hoja_slug"] == "valor_cuota_diario"]
    assert len(hoja) == 1, "No se encontró la hoja de valor cuota en el registro de staging"
    ruta_csv = ("file://" + hoja[0]["ruta"]) if LOCAL else hoja[0]["ruta"]
    crudo = spark.read.option("header", True).csv(ruta_csv).cache()   # texto crudo: todas las columnas string
    filas_crudo = crudo.count()
    mensual = spark.read.parquet(SILVER + "/processed/tabla_analitica_mensual") \
        .select("fecha_mes", "rent_real_integra_12m")

    def pipeline(k):
        rep = crudo.crossJoin(F.broadcast(spark.range(k).withColumnRenamed("id", "replica")))
        grupos = [F.struct(F.lit(f).alias("fondo"), F.lit(a).alias("afp"), F.col(f"c{1 + f * 4 + j}").alias("texto"))
                  for f in range(4) for j, a in enumerate(["Habitat", "Integra", "Prima", "Profuturo"])]
        limpio = (rep.select("replica", F.to_date("c0").alias("fecha"), F.explode(F.array(*grupos)).alias("v"))
                  .select("replica", "fecha", "v.*")
                  .withColumn("valor_cuota", F.col("texto").cast("double")).drop("texto")
                  .filter(F.col("fecha").isNotNull() & (F.col("valor_cuota") > 0))
                  .withColumn("fecha_mes", F.trunc("fecha", "MM")))
        return (limpio.join(mensual, "fecha_mes", "left")      # enriquecimiento como en la integración
                .withColumn("anio", F.year("fecha")))

    filas_base = pipeline(1).count()                     # 1x limpio = filas de sbs_valor_cuota en silver
    p(f"Filas limpias por réplica: {filas_base:,}")
    pipeline(1).write.mode("overwrite").parquet(f"{SALIDA}/escalabilidad/_calentamiento")  # descartado
    resultados, base_por_millon = [], None
    for k in ESCALAS:
        stage0 = ultimo_stage(spark)
        destino = f"{SALIDA}/escalabilidad/escala_{k}x"
        fila = {"escala": k, "filas_crudo": filas_crudo * k}
        t0 = time.perf_counter()
        try:
            df = pipeline(k)
            df.write.mode("overwrite").partitionBy("anio").parquet(destino)          # escritura particionada
            t_escritura = time.perf_counter() - t0
            dup = (spark.read.parquet(destino).groupBy("replica", "fecha", "afp", "fondo").count()
                   .filter("count > 1").count())                                      # control de clave del ETL
            seg = time.perf_counter() - t0
            filas = filas_base * k
            fila.update(estado="ok", filas_procesadas=filas, duplicados_clave=dup,
                        tiempo_escritura_s=round(t_escritura, 2), tiempo_s=round(seg, 2),
                        seg_por_millon=round(seg / (filas / 1e6), 3))
        except Exception as exc:
            seg = time.perf_counter() - t0
            fila.update(estado="fallo", tiempo_s=round(seg, 2), error=str(exc)[:400])
        fila.update(metricas_desde(spark, stage0))
        if fila["estado"] == "ok":
            if base_por_millon is None and fila["filas_procesadas"] >= 1e6:
                base_por_millon = fila["seg_por_millon"]
            fila["degradacion_vs_base"] = round(fila["seg_por_millon"] / base_por_millon, 2) if base_por_millon else None
        resultados.append(fila)
        p(f"[escala {k}x] {fila.get('filas_procesadas', 0):,} filas | {fila['tiempo_s']} s | "
          f"{fila.get('seg_por_millon')} s/M | heap pico {fila['heap_pico_executor_mb']} MB "
          f"(memoria unificada {fila['memoria_unificada_mb']} MB) | spill mem {fila['spill_memoria_mb']} MB, "
          f"disco {fila['spill_disco_mb']} MB | {fila['estado']}")
        if fila["estado"] != "ok" or fila["tiempo_s"] > LIMITE_SEG:
            p(f"Punto de quiebre alcanzado en {k}x; no se prueban escalas mayores.")
            break
    crudo.unpersist()
    return resultados


def main():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("afp-05-benchmark").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    conf = spark.sparkContext.getConf()
    entorno = {"spark": spark.version, "master": spark.sparkContext.master,
               "paralelismo": spark.sparkContext.defaultParallelism,
               "executor_memory": conf.get("spark.executor.memory", "n/d"),
               "executor_cores": conf.get("spark.executor.cores", "n/d"),
               "ui_rest": bool(spark.sparkContext.uiWebUrl)}
    p("Entorno:", entorno)
    t0 = time.perf_counter()
    bc = broadcast_benchmark(spark)
    es = escalabilidad(spark)
    rep = {"fecha": datetime.now(timezone.utc).isoformat(), "entorno": entorno,
           "broadcast": bc, "escalabilidad": es, "duracion_total_s": round(time.perf_counter() - t0, 1)}
    js = json.dumps(rep, ensure_ascii=False, indent=2)
    if LOCAL:
        import os
        os.makedirs(SALIDA[7:], exist_ok=True)
        open(SALIDA[7:] + "/17_benchmark_nube.json", "w").write(js)
    else:
        import boto3
        boto3.client("s3").put_object(Bucket=A["SILVER"], Key="benchmark/17_benchmark_nube.json",
                                      Body=js.encode(), ContentType="application/json")
    p("Reporte: " + SALIDA + "/17_benchmark_nube.json")


if __name__ == "__main__":
    main()
