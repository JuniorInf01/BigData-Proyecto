# -*- coding: utf-8 -*-
"""4.3 Modelo definitivo (AWS Glue Python Shell). Mismos datos, partición y objetivo que el modelo base
(afp_gold.dataset_modelo_mensual, objetivo mensual, entrenamiento ene-2015 a dic-2022, prueba ene-2023 a jul-2026).

Qué agrega frente al modelo base (4.2):
  * Responde los objetivos específicos de 1.5:
      - peso de inflación frente a tipo de cambio (coeficientes estandarizados + importancia por permutación);
      - si las variables del texto de las memorias (freq_*) mejoran el AUC en prueba.
  * Compara dos familias: regresión logística L2 y bosque aleatorio (alternativa no lineal).
  * Ajusta hiperparámetros SOLO con el entrenamiento, por validación cruzada temporal (TimeSeriesSplit, 5 pliegues).
  * El modelo final se elige por AUC medio de validación cruzada, sin mirar la prueba; la prueba se usa una vez.
  * Las freq_* se conocen desde su fecha de publicación (join as-of en silver); sus nulos (meses sin memoria
    publicada) se imputan con la mediana del entrenamiento dentro del pipeline.

Parámetros Glue: --GOLD <bucket>.  Prueba local: python3 afp_modelo_definitivo_job.py <dir_con_csv> <dir_salida>
"""
import os
import sys

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (balanced_accuracy_score, brier_score_loss, confusion_matrix, f1_score,
                             precision_score, recall_score, roc_auc_score)
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

CORTE = pd.Timestamp("2022-12-01")
OBJ = "objetivo_bajo_rendimiento_mes_siguiente"
BLOQUE, REPLICAS, SEMILLA, UMBRAL = 6, 2000, 11, 0.5
INERCIA = ["brecha_mensual_pp", "brecha_mensual_lag1", "brecha_mensual_lag3"]
CONTEXTO = ["ipc_lag1", "tipo_cambio_lag1", "var_tc_12m_pct_lag1", "sistema_lag1"]
TEXTO = ["freq_renta_variable", "freq_exterior", "freq_volatilidad", "freq_retiros", "freq_pandemia_covid",
         "freq_inflacion", "freq_tipo_cambio", "freq_tasa_interes", "freq_alternativos",
         "freq_sostenibilidad_esg", "freq_traspasos", "freq_comisiones"]
CONJUNTOS = {"Contexto": CONTEXTO, "Inercia + contexto": INERCIA + CONTEXTO,
             "Contexto + texto": CONTEXTO + TEXTO, "Inercia + contexto + texto": INERCIA + CONTEXTO + TEXTO}
PROHIBIDAS = {"brecha_mensual_siguiente_pp", OBJ, "anio", "fecha_mes", "memoria_ejercicio_usada"}
MESES = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]
mes = lambda ts: "%s-%d" % (MESES[ts.month - 1], ts.year)

FAMILIAS = {
    "Regresión logística L2": (
        lambda: Pipeline([("imp", SimpleImputer(strategy="median")),
                          ("esc", StandardScaler()),
                          ("clf", LogisticRegression(max_iter=2000))]),
        {"clf__C": [0.01, 0.1, 1.0, 10.0]}),
    "Bosque aleatorio": (
        lambda: Pipeline([("imp", SimpleImputer(strategy="median")),
                          ("clf", RandomForestClassifier(n_estimators=300, random_state=SEMILLA, n_jobs=1))]),
        {"clf__max_depth": [2, 3, 5], "clf__min_samples_leaf": [5, 10]}),
}


def bloques(n, rng):
    k = int(np.ceil(n / BLOQUE))
    ini = rng.integers(0, n - BLOQUE + 1, k)
    return np.concatenate([np.arange(i, i + BLOQUE) for i in ini])[:n]


def ic_auc(y, s, rng):
    v = []
    for _ in range(REPLICAS):
        i = bloques(len(y), rng)
        if len(np.unique(y[i])) == 2:
            v.append(roc_auc_score(y[i], s[i]))
    return np.percentile(v, [2.5, 97.5]), np.array(v)


def metricas(y, p):
    pred = (p >= UMBRAL).astype(int)
    tn, fp, fn, tp = confusion_matrix(y, pred, labels=[0, 1]).ravel()
    return ("bacc %.3f | precisión %.3f | recall %.3f | F1 %.3f | TN %d FP %d FN %d TP %d"
            % (balanced_accuracy_score(y, pred), precision_score(y, pred, zero_division=0), recall_score(y, pred),
               f1_score(y, pred), tn, fp, fn, tp))


def main(carpeta, salida):
    os.makedirs(salida, exist_ok=True)
    L = []
    P = lambda *x: (print(*x), L.append(" ".join(str(i) for i in x)))
    t = pd.read_csv(os.path.join(carpeta, "dataset_modelo_mensual.csv"), parse_dates=["fecha_mes"]).set_index("fecha_mes").sort_index()
    lab = t[t[OBJ].notna()].copy()
    for cols in CONJUNTOS.values():
        assert not (set(cols) & PROHIBIDAS)
    tr, te = lab[lab.index <= CORTE], lab[lab.index > CORTE]
    y_tr, y_te = tr[OBJ].astype(int).values, te[OBJ].astype(int).values
    tscv = TimeSeriesSplit(n_splits=5)
    P("== DATOS ==")
    P("entrenamiento: %s a %s, %d meses | prueba: %s a %s, %d meses" % (mes(tr.index[0]), mes(tr.index[-1]), len(tr),
                                                                        mes(te.index[0]), mes(te.index[-1]), len(te)))
    P("meses de entrenamiento sin memoria publicada (freq_* nulas, imputadas con la mediana): %d" % int(tr[TEXTO[0]].isna().sum()))
    P("validación cruzada: TimeSeriesSplit de 5 pliegues dentro del entrenamiento; métrica AUC")

    P("\n== AJUSTE DE HIPERPARÁMETROS Y RESULTADOS (selección por AUC de validación cruzada) ==")
    filas = []
    for fam, (fab, grid) in FAMILIAS.items():
        for cj, cols in CONJUNTOS.items():
            gs = GridSearchCV(fab(), grid, cv=tscv, scoring="roc_auc", n_jobs=1).fit(tr[cols], y_tr)
            p = gs.best_estimator_.predict_proba(te[cols])[:, 1]
            filas.append(dict(familia=fam, conjunto=cj, cols=cols, cv=gs.best_score_, params=gs.best_params_,
                              modelo=gs.best_estimator_, p=p, auc=roc_auc_score(y_te, p), brier=brier_score_loss(y_te, p)))
            P("%-24s %-28s CV AUC %.3f | hiperparámetros %s | AUC prueba %.3f | Brier %.3f"
              % (fam, cj, gs.best_score_, gs.best_params_, filas[-1]["auc"], filas[-1]["brier"]))
    pers = -te.brecha_mensual_pp.values
    P("%-24s %-28s %s | AUC prueba %.3f" % ("Regla de persistencia", "signo brecha actual", " " * 37, roc_auc_score(y_te, pers)))

    final = max(filas, key=lambda f: f["cv"])
    P("\n== MODELO DEFINITIVO (mayor AUC de validación cruzada) ==")
    P("%s · %s · %s" % (final["familia"], final["conjunto"], final["params"]))
    rng = np.random.default_rng(SEMILLA)
    (lo, hi), _ = ic_auc(y_te, final["p"], rng)
    P("AUC prueba %.3f [IC95 %.3f a %.3f] | %s | Brier %.3f" % (final["auc"], lo, hi, metricas(y_te, final["p"]), final["brier"]))
    rng = np.random.default_rng(SEMILLA)
    _, va = ic_auc(y_te, final["p"], rng)
    P("probabilidad (réplicas bootstrap) de que el AUC supere 0.70: %.1f %%" % (100 * (va > 0.70).mean()))

    P("\n== ¿EL TEXTO DE LAS MEMORIAS MEJORA EL AUC EN PRUEBA? (misma familia y mismos hiperparámetros elegidos por CV) ==")
    for fam in FAMILIAS:
        for sin, con in [("Contexto", "Contexto + texto"), ("Inercia + contexto", "Inercia + contexto + texto")]:
            a = next(f for f in filas if f["familia"] == fam and f["conjunto"] == sin)
            b = next(f for f in filas if f["familia"] == fam and f["conjunto"] == con)
            rng = np.random.default_rng(SEMILLA); d = []
            for _ in range(REPLICAS):
                i = bloques(len(y_te), rng)
                if len(np.unique(y_te[i])) == 2:
                    d.append(roc_auc_score(y_te[i], b["p"][i]) - roc_auc_score(y_te[i], a["p"][i]))
            P("%-24s %-20s -> %-28s ΔAUC %+.3f [IC95 %+.3f a %+.3f]" % (fam, sin, con, b["auc"] - a["auc"], *np.percentile(d, [2.5, 97.5])))

    P("\n== INFLACIÓN FRENTE A TIPO DE CAMBIO ==")
    log = next(f for f in filas if f["familia"].startswith("Regresión") and f["conjunto"] == "Inercia + contexto")
    coef = dict(zip(log["cols"], log["modelo"].named_steps["clf"].coef_[0]))
    P("Coeficientes estandarizados (logística, inercia + contexto; log-odds de bajo rendimiento):")
    for c in sorted(coef, key=lambda c: -abs(coef[c])):
        P("  %-22s %+.3f" % (c, coef[c]))
    pi = permutation_importance(final["modelo"], te[final["cols"]], y_te, scoring="roc_auc", n_repeats=50, random_state=SEMILLA)
    P("Importancia por permutación en prueba del modelo definitivo (caída de AUC al desordenar la variable):")
    for i in np.argsort(-pi.importances_mean):
        P("  %-24s %+.3f ± %.3f" % (final["cols"][i], pi.importances_mean[i], pi.importances_std[i]))

    out = pd.DataFrame({"fecha_mes": te.index.strftime("%Y-%m-%d"), "prob_bajo_rendimiento": final["p"],
                        "prediccion": (final["p"] >= UMBRAL).astype(int), "real": y_te})
    out.to_csv(os.path.join(salida, "predicciones_definitivo.csv"), index=False)
    import sklearn
    P("\nversiones: pandas %s, numpy %s, scikit-learn %s" % (pd.__version__, np.__version__, sklearn.__version__))
    open(os.path.join(salida, "cifras_modelo_definitivo.txt"), "w", encoding="utf-8").write("\n".join(L) + "\n")


def glue():
    import io, datetime, boto3
    a = {sys.argv[i][2:]: sys.argv[i + 1] for i in range(len(sys.argv) - 1) if sys.argv[i].startswith("--")}
    s3, gold = boto3.client("s3"), a["GOLD"]
    partes = [pd.read_parquet(io.BytesIO(s3.get_object(Bucket=gold, Key=o["Key"])["Body"].read()))
              for o in s3.list_objects_v2(Bucket=gold, Prefix="dataset_modelo_mensual/").get("Contents", [])]
    df = pd.concat(partes).sort_values("fecha_mes")
    if len(df) != 140:
        raise RuntimeError("dataset_modelo_mensual con %d filas; se esperaban 140" % len(df))
    os.makedirs("/tmp/in", exist_ok=True)
    df.to_csv("/tmp/in/dataset_modelo_mensual.csv", index=False)
    main("/tmp/in", "/tmp/out")
    corrida = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    for f in os.listdir("/tmp/out"):
        s3.put_object(Bucket=gold, Key="modelo/definitivo/%s/%s" % (corrida, f), Body=open(os.path.join("/tmp/out", f), "rb").read())
    print("Resultados en s3://%s/modelo/definitivo/%s/" % (gold, corrida))


if __name__ == "__main__":
    glue() if "--GOLD" in sys.argv else main(sys.argv[1], sys.argv[2])
