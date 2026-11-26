"""Lee los event logs de Spark del job afp-05-benchmark (S3) y calcula, por escala de la prueba de
escalabilidad, la memoria pico del executor (JVM heap), la memoria de ejecución pico, el spill y el GC.
Uso en CloudShell:  python3 afp_benchmark_memoria.py afp-integra-silver-672469410615
"""
import gzip, io, json, sys, boto3
from collections import defaultdict

bucket = sys.argv[1]
s3 = boto3.client("s3")
keys = []
for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="benchmark/sparkui/"):
    keys += [(o["LastModified"], o["Key"]) for o in pg.get("Contents", [])]
if not keys:
    sys.exit("No hay event logs en benchmark/sparkui/")
# Glue deja el mismo log con varios nombres (jr_..., spark-application-..., .inprogress): se usa uno solo
completos = [(t, k) for t, k in keys if not k.endswith(".inprogress")]
keys = [max(completos or keys)[1]]
eventos = []
for k in keys:
    data = s3.get_object(Bucket=bucket, Key=k)["Body"].read()
    if data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    for linea in data.decode("utf-8", errors="replace").splitlines():
        if linea.startswith("{"):
            try:
                eventos.append(json.loads(linea))
            except ValueError:
                pass
print("archivos:", len(keys), "| eventos:", len(eventos))

st = defaultdict(lambda: dict(escritas=0, heap=0, ejec=0, spill_mem=0, spill_disco=0, gc=0, shuffle=0, ini=None))
for e in eventos:
    if e.get("Event") == "SparkListenerStageSubmitted":
        st[e["Stage Info"]["Stage ID"]]["ini"] = e["Stage Info"].get("Submission Time")
    if e.get("Event") != "SparkListenerTaskEnd":
        continue
    s = st[e["Stage ID"]]
    m = e.get("Task Metrics") or {}
    s["escritas"] += (m.get("Output Metrics") or {}).get("Records Written", 0)
    s["ejec"] = max(s["ejec"], m.get("Peak Execution Memory", 0))
    s["spill_mem"] += m.get("Memory Bytes Spilled", 0)
    s["spill_disco"] += m.get("Disk Bytes Spilled", 0)
    s["gc"] += m.get("JVM GC Time", 0)
    s["shuffle"] += (m.get("Shuffle Write Metrics") or {}).get("Shuffle Bytes Written", 0)
    s["heap"] = max(s["heap"], (e.get("Task Executor Metrics") or {}).get("JVMHeapMemory", 0))

orden = sorted((v["ini"] or 0, sid) for sid, v in st.items())
FILAS = 76059
escalas = [1, 10, 100, 300, 1000, 2000, 3000]
# etapa de escritura de cada escala = la que escribió exactamente 76 059 x k filas
marcas = []
for k in escalas:
    cand = [sid for _, sid in orden if st[sid]["escritas"] == FILAS * k]
    if cand:
        marcas.append((k, cand[-1]))
print("\nescala | heap pico executor (MB) | mem. ejecución pico/tarea (MB) | spill memoria (MB) | spill disco (MB) | shuffle (MB) | GC (s)")
ids = [sid for _, sid in orden]
for i, (k, sid) in enumerate(marcas):
    fin = ids.index(marcas[i + 1][1]) if i + 1 < len(marcas) else len(ids)
    tramo = ids[ids.index(sid):fin]          # escritura + control de duplicados de esa escala
    mb = lambda c: sum(st[x][c] for x in tramo) / 2**20
    print("%5dx | %8.0f | %8.0f | %10.1f | %10.1f | %8.1f | %6.1f" % (
        k, max(st[x]["heap"] for x in tramo) / 2**20, max(st[x]["ejec"] for x in tramo) / 2**20,
        mb("spill_mem"), mb("spill_disco"), mb("shuffle"), sum(st[x]["gc"] for x in tramo) / 1000))
