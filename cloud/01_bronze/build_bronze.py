"""Arma la zona bronce con la misma convención de rutas que el cuaderno 01
(raw/<familia>/<conjunto>/ingest_date=AAAA-MM-DD/<archivo>) y un manifiesto
JSONL con SHA-256. No modifica el contenido de ningún archivo."""
import hashlib, json, re, shutil, sys, unicodedata
from pathlib import Path

SRC = Path(sys.argv[1]); OUT = Path(sys.argv[2]); FECHA = sys.argv[3]

def fix(name):  # el unzip dejó los acentos como #UXXXX
    return re.sub(r"#U([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), name)

def slug(t):
    s = unicodedata.normalize("NFKD", str(t))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub("[^a-z0-9]+", "_", s.lower()).strip("_")

def sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()

if OUT.exists():
    shutil.rmtree(OUT)
files = sorted(p for p in SRC.rglob("*") if p.is_file())
assert len(files) == 65, len(files)
man = []
for p in files:
    rel = [fix(x) for x in p.relative_to(SRC).parts]
    nombre = rel[-1]; stem = Path(nombre).stem; suf = Path(nombre).suffix.lower()
    if rel[0] == "Datos Estructurados":
        fam = "sbs" if suf in (".xls", ".xlsx") else "bcrp/csv"; conj = slug(stem)
    elif rel[0] == "Datos Semiestructurados":
        fam = "bcrp/json"; conj = slug(stem)
    elif rel[0] == "Datos No estructurados":
        fam = "afp_memorias/" + slug(rel[1]); conj = "memorias_anuales"
    else:
        fam = "control"; conj = slug(rel[1]) if len(rel) > 1 else "registro_descargas"
        conj = slug(Path(nombre).stem)
    key = f"{fam}/{conj}/ingest_date={FECHA}/{nombre}"
    dst = OUT / key
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(p, dst)
    h = sha(p)
    assert h == sha(dst)
    man.append({"origen_relativo": "/".join(rel), "s3_key": key, "bytes": p.stat().st_size,
                "sha256": h, "ingest_date": FECHA})
mpath = OUT / "_manifests" / f"01_manifiesto_ingesta_{FECHA}.jsonl"
mpath.parent.mkdir(parents=True)
with mpath.open("w", encoding="utf-8") as f:
    for r in man:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
print("archivos:", len(man), "bytes:", sum(r["bytes"] for r in man))
