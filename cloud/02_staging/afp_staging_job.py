"""AWS Glue Python Shell — Preparación (bronze -> staging).
Equivale al cuaderno 03 / afp_preparacion.py del hito parcial, sin LibreOffice
ni pdftotext: separa las hojas de los libros SBS en CSV (c0..cN) y extrae el
texto de las 48 memorias. Verifica cada archivo contra el manifiesto SHA-256
antes de procesarlo y deja un registro JSON. No modifica la zona bronze.

Parámetros del job: --BRONZE <bucket>  --SILVER <bucket>
Prueba local:  python afp_staging_job.py --LOCAL_BRONZE dir --LOCAL_SILVER dir
"""
import csv, datetime, hashlib, io, json, re, struct, sys, unicodedata, zipfile
from xml.etree import ElementTree as ET


# ------------------------------------------------------------ almacenamiento
def args():
    a = {}
    for i, x in enumerate(sys.argv):
        if x.startswith("--") and i + 1 < len(sys.argv):
            a[x[2:]] = sys.argv[i + 1]
    return a


class S3:
    def __init__(self, bronze, silver):
        import boto3
        self.s3 = boto3.client("s3"); self.b = bronze; self.s = silver

    def list_bronze(self):
        keys, tok = [], None
        while True:
            kw = {"Bucket": self.b}
            if tok: kw["ContinuationToken"] = tok
            r = self.s3.list_objects_v2(**kw)
            keys += [o["Key"] for o in r.get("Contents", [])]
            if not r.get("IsTruncated"): return keys
            tok = r["NextContinuationToken"]

    def read(self, key):
        return self.s3.get_object(Bucket=self.b, Key=key)["Body"].read()

    def write(self, key, data, ctype="text/plain; charset=utf-8"):
        self.s3.put_object(Bucket=self.s, Key=key, Body=data, ContentType=ctype)
        return f"s3://{self.s}/{key}"


class Local:
    def __init__(self, bronze, silver):
        from pathlib import Path
        self.b = Path(bronze); self.s = Path(silver)

    def list_bronze(self):
        return [str(p.relative_to(self.b)) for p in self.b.rglob("*") if p.is_file()]

    def read(self, key):
        return (self.b / key).read_bytes()

    def write(self, key, data, ctype=None):
        p = self.s / key; p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes(data)
        return str(p)


# ------------------------------------------------------------ utilidades
def slug(t):
    s = unicodedata.normalize("NFKD", str(t))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub("[^a-z0-9]+", "_", s.lower()).strip("_")


def _txt(v):
    if v is None: return ""
    if isinstance(v, datetime.datetime): return v.strftime("%Y-%m-%d")
    if isinstance(v, datetime.date): return v.isoformat()
    if isinstance(v, float) and v.is_integer(): return str(int(v))
    return str(v).strip()


# ------------------------------------------------------------ Excel
def libro_a_hojas(data):
    if data[:2] == b"PK":  # XLSX con extensión .XLS
        from openpyxl import load_workbook
        wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        return "xlsx_interno_ext_xls", [
            (ws.title, [[_txt(x) for x in r] for r in ws.iter_rows(values_only=True)], ws.max_column or 0)
            for ws in wb]
    import xlrd  # XLS binario (BIFF)
    wb = xlrd.open_workbook(file_contents=data)
    out = []
    for sh in wb.sheets():
        filas = []
        for r in range(sh.nrows):
            fila = []
            for c in range(sh.ncols):
                cell = sh.cell(r, c); v = cell.value
                if cell.ctype == xlrd.XL_CELL_DATE:
                    v = xlrd.xldate.xldate_as_datetime(v, wb.datemode)
                elif cell.ctype == xlrd.XL_CELL_EMPTY:
                    v = None
                fila.append(_txt(v))
            filas.append(fila)
        out.append((sh.name, filas, sh.ncols))
    return "xls_legacy_biff", out


def hoja_a_csv(filas, ncols):
    buf = io.StringIO(); w = csv.writer(buf)
    w.writerow(["c" + str(i) for i in range(ncols)])
    for f in filas:
        w.writerow(f + [""] * (ncols - len(f)))
    return buf.getvalue().encode("utf-8")


# ------------------------------------------------------------ documentos
def pdf_a_texto(data):
    from pypdf import PdfReader
    return "\n".join((p.extract_text() or "") for p in PdfReader(io.BytesIO(data)).pages)


def docx_a_texto(data):
    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    with zipfile.ZipFile(io.BytesIO(data)) as f:
        raiz = ET.fromstring(f.read("word/document.xml"))
    pars = ["".join(t.text or "" for t in p.findall(".//w:t", ns)) for p in raiz.findall(".//w:p", ns)]
    return "\n".join(x for x in pars if x)


def doc_a_texto(data):
    """Word 97-2003 binario: lee la tabla de piezas (CLX) indicada por el FIB."""
    import olefile
    ole = olefile.OleFileIO(io.BytesIO(data))
    wd = ole.openstream("WordDocument").read()
    flags = struct.unpack_from("<H", wd, 0x0A)[0]
    tabla = ole.openstream("1Table" if flags & 0x0200 else "0Table").read()
    fc_clx, lcb_clx = struct.unpack_from("<II", wd, 0x01A2)
    clx = tabla[fc_clx:fc_clx + lcb_clx]; i = 0
    while clx[i] == 0x01:
        i += 3 + struct.unpack_from("<H", clx, i + 1)[0]
    if clx[i] != 0x02:
        raise RuntimeError("CLX sin tabla de piezas")
    lcb = struct.unpack_from("<I", clx, i + 1)[0]
    plc = clx[i + 5:i + 5 + lcb]; n = (lcb - 4) // 12
    cps = struct.unpack_from("<%dI" % (n + 1), plc, 0)
    partes = []
    for k in range(n):
        fc = struct.unpack_from("<I", plc, 4 * (n + 1) + 8 * k + 2)[0]
        largo = cps[k + 1] - cps[k]
        if fc & 0x40000000:
            off = (fc & ~0x40000000) // 2
            partes.append(wd[off:off + largo].decode("cp1252", errors="replace"))
        else:
            partes.append(wd[fc:fc + 2 * largo].decode("utf-16-le", errors="replace"))
    t = "".join(partes)
    t = re.sub("\x13[^\x13\x14\x15]*\x14", "", t)
    t = re.sub("[\x13\x14\x15]", "", t)
    t = t.replace("\r", "\n").replace("\x07", "\t").replace("\x0b", "\n").replace("\x0c", "\n")
    return re.sub("[\x00-\x08\x0e-\x1f]", "", t)


def documento_a_texto(nombre, data):
    ext = nombre.lower().rsplit(".", 1)[-1]
    if ext == "pdf": return pdf_a_texto(data)
    if ext == "docx": return docx_a_texto(data)
    if ext == "doc": return doc_a_texto(data)
    raise RuntimeError("Extensión desconocida: " + ext)


# ------------------------------------------------------------ job
def main():
    a = args()
    st = Local(a["LOCAL_BRONZE"], a["LOCAL_SILVER"]) if "LOCAL_BRONZE" in a else S3(a["BRONZE"], a["SILVER"])
    claves = st.list_bronze()
    man_key = sorted(k for k in claves if k.startswith("_manifests/"))[-1]
    manifiesto = [json.loads(l) for l in st.read(man_key).decode("utf-8").splitlines() if l.strip()]
    if len(manifiesto) != 65:
        raise RuntimeError(f"Manifiesto con {len(manifiesto)} archivos; se esperaban 65")
    por_key = {r["s3_key"]: r for r in manifiesto}

    def leer_verificado(key):
        data = st.read(key)
        if hashlib.sha256(data).hexdigest() != por_key[key]["sha256"]:
            raise RuntimeError("SHA-256 no coincide con el manifiesto: " + key)
        return data

    libros, hojas, documentos, errores = [], [], [], []

    # 1) libros SBS -> una hoja por CSV
    for key in sorted(k for k in por_key if k.startswith("sbs/")):
        nombre = key.rsplit("/", 1)[-1]; libro = slug(nombre.rsplit(".", 1)[0])
        formato, hs = libro_a_hojas(leer_verificado(key))
        libros.append({"archivo_original": nombre, "formato_detectado": formato, "hojas": [h[0] for h in hs]})
        for titulo, filas, ncols in hs:
            ruta = st.write(f"staging/sbs/{libro}/{slug(titulo)}.csv", hoja_a_csv(filas, ncols), "text/csv")
            hojas.append({"libro": libro, "hoja": titulo, "hoja_slug": slug(titulo),
                          "filas_origen": len(filas), "columnas_origen": ncols, "ruta": ruta})

    # 2) memorias -> texto
    ctrl = next(k for k in por_key if k.startswith("control/manifiesto_memorias/"))
    meta = {r["ruta"].replace("\\", "/"): r for r in
            csv.DictReader(io.StringIO(leer_verificado(ctrl).decode("utf-8-sig")))}
    for key in sorted(k for k in por_key if k.startswith("afp_memorias/")):
        rel = por_key[key]["origen_relativo"]; m = meta.get(rel, {})
        nombre = key.rsplit("/", 1)[-1]
        anio = m.get("anio_ejercicio") or (re.search(r"20(?:1[5-9]|2[0-5])", nombre) or [None])[0]
        afp = rel.split("/")[1].replace("Memoria ", "").strip()
        try:
            if not anio:
                raise RuntimeError("Sin año en nombre ni manifiesto; no se inventa")
            texto = documento_a_texto(nombre, leer_verificado(key))
            if len(texto.strip()) < 100:
                raise RuntimeError("Texto insuficiente (¿PDF escaneado?); no se aplica OCR automático")
            ruta = st.write(f"staging/documentos/{slug(afp)}/{slug(nombre.rsplit('.', 1)[0])}.txt",
                            texto.encode("utf-8"))
            documentos.append({"afp": afp, "anio_ejercicio": int(anio), "parte": int(m.get("parte", 1)),
                               "disponible_desde_regla": m.get("disponible_desde_regla", f"{int(anio) + 1}-04-01"),
                               "archivo_original": rel, "ruta": ruta, "caracteres_extraidos": len(texto),
                               "extractor": nombre.lower().rsplit(".", 1)[-1]})
        except Exception as exc:
            errores.append({"archivo": rel, "error": str(exc)})

    registro = {"fecha_ejecucion": datetime.datetime.now(datetime.timezone.utc).isoformat(), "manifiesto": man_key,
                "libros": libros, "hojas": hojas, "documentos": documentos, "errores": errores}
    st.write("staging/_registro/03_staging_registro.json",
             json.dumps(registro, ensure_ascii=False, indent=2).encode("utf-8"), "application/json")
    print(f"Libros: {len(libros)} | hojas: {len(hojas)} | documentos: {len(documentos)} | errores: {len(errores)}")
    if len(libros) != 3 or len(hojas) != 15 or len(documentos) != 48 or errores:
        raise RuntimeError("Preparación incompleta; revisar staging/_registro/03_staging_registro.json")


if __name__ == "__main__":
    main()
