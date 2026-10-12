# -*- coding: utf-8 -*-
"""4.2 Modelo base: entrena y evalúa el clasificador del mes siguiente y genera figuras y cifras.

Uso:
    python3 modelo_base_4_2.py <carpeta_con_los_CSV> <carpeta_de_salida>

Lee:     tabla_analitica_mensual.csv (zona procesada exportada a CSV)
Escribe: fig7_auc_modelos.png, fig8_probabilidad_prueba.png, cifras_4_2.txt

Problema: clasificación binaria. Etiqueta = objetivo_bajo_rendimiento_mes_siguiente
(1 si la rentabilidad real a 12 meses de Integra, Fondo 2, queda por debajo del promedio
simple de Habitat, Prima y Profuturo en el mes siguiente).

Reglas de diseño (declaradas antes de ver los resultados):
  * Partición cronológica: entrenamiento hasta CORTE, prueba después. Nada aleatorio.
  * Solo variables conocidas al cierre del mes t. Se excluyen la brecha del mes siguiente
    (es la etiqueta en continuo), la propia etiqueta, el año y las variables con rezago
    de publicación incierto (afiliados, valor cuota, textos).
  * El escalado se ajusta solo con el entrenamiento.
  * Métrica principal: AUC en prueba. Intervalos por bootstrap de bloques (los meses
    consecutivos no son independientes).
"""
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (accuracy_score, balanced_accuracy_score, brier_score_loss, confusion_matrix,
                             f1_score, precision_score, recall_score, roc_auc_score)
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

CORTE = pd.Timestamp("2022-12-01")
OBJ = "objetivo_bajo_rendimiento_mes_siguiente"
BLOQUE, REPLICAS, SEMILLA = 6, 2000, 11
UMBRAL = 0.5

INERCIA = ["brecha_real_pp", "brecha_lag1", "brecha_lag3"]
CONTEXTO = ["ipc_lag1", "tipo_cambio_lag1", "var_tc_12m_pct_lag1", "sistema_lag1"]
MODELOS = {
    "Inercia (brecha t, t−1, t−3)": INERCIA,
    "Contexto (IPC, tipo de cambio, sistema)": CONTEXTO,
    "Inercia + contexto (modelo base)": INERCIA + CONTEXTO,
}
BASE = "Inercia + contexto (modelo base)"
PERSIST = "Regla de persistencia (signo de la brecha actual)"
MAYOR = "Clase mayoritaria del entrenamiento"
PROHIBIDAS = {"brecha_mes_siguiente_pp", OBJ, "anio", "fuente_sistema"}

AZUL, NARANJA, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
GRIS, GRIS_CLARO, TINTA, TINTA2 = "#898781", "#e1e0d9", "#0b0b0b", "#52514e"
plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 8.5, "axes.edgecolor": GRIS_CLARO,
    "axes.labelcolor": TINTA2, "xtick.color": TINTA2, "ytick.color": TINTA2,
    "axes.spines.top": False, "axes.spines.right": False, "legend.frameon": False,
})
MESES = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]
mes = lambda ts: "%s-%d" % (MESES[ts.month - 1], ts.year)


def nuevo_modelo():
    return make_pipeline(StandardScaler(), LogisticRegression(C=1.0, max_iter=1000))


def auc_seguro(y, s):
    return roc_auc_score(y, s) if len(np.unique(y)) == 2 else np.nan


def bloques(n, rng):
    k = int(np.ceil(n / BLOQUE))
    ini = rng.integers(0, n - BLOQUE + 1, k)
    return np.concatenate([np.arange(i, i + BLOQUE) for i in ini])[:n]


def metricas(y, p, umbral=UMBRAL):
    pred = (p >= umbral).astype(int)
    tn, fp, fn, tp = confusion_matrix(y, pred, labels=[0, 1]).ravel()
    return dict(acc=accuracy_score(y, pred), bacc=balanced_accuracy_score(y, pred),
                prec=precision_score(y, pred, zero_division=0), rec=recall_score(y, pred), f1=f1_score(y, pred),
                tn=int(tn), fp=int(fp), fn=int(fn), tp=int(tp))


def evaluar(lab, corte, cols_por_modelo, out=None):
    """Entrena con meses <= corte y evalúa con meses > corte. Devuelve scores de prueba por modelo."""
    tr, te = lab[lab.index <= corte], lab[lab.index > corte]
    y_tr, y_te = tr[OBJ].astype(int).values, te[OBJ].astype(int).values
    res = {}
    for nombre, cols in cols_por_modelo.items():
        assert not (set(cols) & PROHIBIDAS), "variable no disponible al predecir: %s" % (set(cols) & PROHIBIDAS)
        ok = tr[cols].notna().all(axis=1)
        assert te[cols].notna().all().all(), "hay nulos en prueba"
        m = nuevo_modelo().fit(tr.loc[ok, cols], tr.loc[ok, OBJ].astype(int))
        res[nombre] = dict(modelo=m, p_te=m.predict_proba(te[cols])[:, 1], p_tr=m.predict_proba(tr.loc[ok, cols])[:, 1],
                           y_tr=tr.loc[ok, OBJ].astype(int).values, n_tr=int(ok.sum()), cols=cols)
    res[PERSIST] = dict(p_te=-te.brecha_real_pp.values, p_tr=-tr.brecha_real_pp.values, y_tr=y_tr, n_tr=len(tr), cols=["brecha_real_pp"],
                        clase=(te.brecha_real_pp.values < 0).astype(int), clase_tr=(tr.brecha_real_pp.values < 0).astype(int))
    mayoritaria = int(round(y_tr.mean()))
    res[MAYOR] = dict(p_te=np.full(len(te), float(mayoritaria)), p_tr=np.full(len(tr), float(mayoritaria)), y_tr=y_tr, n_tr=len(tr), cols=[],
                      clase=np.full(len(te), mayoritaria), clase_tr=np.full(len(tr), mayoritaria))
    return tr, te, y_tr, y_te, res


def main(carpeta, salida):
    os.makedirs(salida, exist_ok=True)
    t = pd.read_csv(os.path.join(carpeta, "tabla_analitica_mensual.csv"), parse_dates=["fecha_mes"]).set_index("fecha_mes")
    lab = t.dropna(subset=[OBJ])
    out = []
    P = lambda s="": out.append(s)

    tr, te, y_tr, y_te, res = evaluar(lab, CORTE, MODELOS)
    P("== PARTICIÓN CRONOLÓGICA ==")
    P("meses con etiqueta: %d (%s a %s); sin etiqueta: %d (%s)" % (len(lab), mes(lab.index.min()), mes(lab.index.max()),
                                                                   t[OBJ].isna().sum(), mes(t.index[t[OBJ].isna()][0])))
    P("entrenamiento: %s a %s, %d meses, %d positivos (%.1f %%)" % (mes(tr.index.min()), mes(tr.index.max()), len(tr), y_tr.sum(), 100 * y_tr.mean()))
    P("prueba: %s a %s, %d meses, %d positivos (%.1f %%), %d negativos" % (mes(te.index.min()), mes(te.index.max()), len(te), y_te.sum(), 100 * y_te.mean(), (y_te == 0).sum()))
    P("variables prohibidas (no entran): %s" % sorted(PROHIBIDAS))
    for n_, c_ in MODELOS.items():
        P("%s -> %s" % (n_, c_))
    P("filas de entrenamiento usadas por modelo (se descartan las sin rezago disponible): " + ", ".join(
        "%s: %d" % (k, v["n_tr"]) for k, v in res.items() if "modelo" in v))
    P("modelo: estandarización (media y desv. del entrenamiento) + regresión logística L2, C = 1.0; umbral %.2f" % UMBRAL)

    # --------------------------------------------------- métricas en prueba
    P("\n== MÉTRICAS EN PRUEBA ==")
    orden = [MAYOR, PERSIST] + list(MODELOS)
    tabla = {}
    for nombre in orden:
        r = res[nombre]
        auc_tr = auc_seguro(r["y_tr"], r["p_tr"])
        auc_te = auc_seguro(y_te, r["p_te"])
        if "modelo" in r:
            mt = metricas(y_te, r["p_te"])
            brier = brier_score_loss(y_te, r["p_te"])
        else:
            mt = metricas(y_te, r["clase"].astype(float))
            brier = np.nan
        tabla[nombre] = dict(auc_tr=auc_tr, auc_te=auc_te, brier=brier, **mt)
        P("%-52s AUC entren. %.3f | AUC prueba %.3f | exactitud %.3f | exact. balanceada %.3f | precisión %.3f | recall %.3f | F1 %.3f | TN %d FP %d FN %d TP %d%s" % (
            nombre, auc_tr, auc_te, mt["acc"], mt["bacc"], mt["prec"], mt["rec"], mt["f1"], mt["tn"], mt["fp"], mt["fn"], mt["tp"],
            "" if brier != brier else " | Brier %.3f" % brier))

    # ------------------------------------------ intervalos: bootstrap de bloques
    rng = np.random.default_rng(SEMILLA)
    n = len(te)
    boots = {k: [] for k in orden}
    difs = {k: [] for k in orden}
    for _ in range(REPLICAS):
        idx = bloques(n, rng)
        yy = y_te[idx]
        if len(np.unique(yy)) < 2:
            continue
        a = {k: roc_auc_score(yy, res[k]["p_te"][idx]) if k != MAYOR else 0.5 for k in orden}
        for k in orden:
            boots[k].append(a[k])
            difs[k].append(a[k] - a[PERSIST])
    ci = {k: np.percentile(boots[k], [2.5, 97.5]) for k in orden}
    P("\n== INTERVALOS (bootstrap de bloques de %d meses, %d réplicas, 95 %%) ==" % (BLOQUE, REPLICAS))
    for k in orden:
        P("AUC en prueba %-52s %.3f  [%.3f a %.3f]" % (k, tabla[k]["auc_te"], ci[k][0], ci[k][1]))
    for k in list(MODELOS):
        d = np.array(difs[k])
        P("AUC(%s) − AUC(persistencia): %+.3f [%+.3f a %+.3f]; réplicas con diferencia > 0: %.1f %%" % (
            k, tabla[k]["auc_te"] - tabla[PERSIST]["auc_te"], *np.percentile(d, [2.5, 97.5]), 100 * (d > 0).mean()))
    P("probabilidad (réplicas) de que el AUC del modelo base supere 0.70: %.1f %%" % (100 * (np.array(boots[BASE]) > 0.70).mean()))

    # -------------------------------------------- coeficientes del modelo base
    P("\n== COEFICIENTES DEL MODELO BASE (variables estandarizadas; log-odds de 'bajo rendimiento') ==")
    m = res[BASE]["modelo"]
    coef = m[-1].coef_[0]
    for c_, b_ in sorted(zip(INERCIA + CONTEXTO, coef), key=lambda z: -abs(z[1])):
        P("%-22s %+.3f" % (c_, b_))
    P("intercepto %+.3f" % m[-1].intercept_[0])

    # -------------------------- validación de avance temporal (walk-forward)
    P("\n== VALIDACIÓN DE AVANCE TEMPORAL (reajuste cada mes con los meses cuya etiqueta ya se conocía) ==")
    wf = {k: [] for k in list(MODELOS)}
    for fecha in te.index:
        pasado = lab[lab.index < fecha]
        for nombre, cols in MODELOS.items():
            ok = pasado[cols].notna().all(axis=1)
            mm = nuevo_modelo().fit(pasado.loc[ok, cols], pasado.loc[ok, OBJ].astype(int))
            wf[nombre].append(mm.predict_proba(te.loc[[fecha], cols])[:, 1][0])
    for nombre in MODELOS:
        p_ = np.array(wf[nombre])
        mt = metricas(y_te, p_)
        P("%-52s AUC %.3f | exactitud %.3f | precisión %.3f | recall %.3f" % (nombre, roc_auc_score(y_te, p_), mt["acc"], mt["prec"], mt["rec"]))
    P("(la regla de persistencia no se reajusta: AUC %.3f)" % tabla[PERSIST]["auc_te"])

    # --------------------------------------------- sensibilidad al corte
    P("\n== SENSIBILIDAD AL CORTE (AUC en prueba) ==")
    P("%-10s %-10s %-10s %-14s %-12s %-18s" % ("corte", "n_train", "n_prueba", "persistencia", "modelo base", "contexto solo"))
    for c in ["2021-12-01", "2022-06-01", "2022-12-01", "2023-06-01"]:
        cc = pd.Timestamp(c)
        _, te2, _, y2, r2 = evaluar(lab, cc, MODELOS)
        P("%-10s %-10d %-10d %-14.3f %-12.3f %-18.3f" % (mes(cc), int((lab.index <= cc).sum()), len(te2),
                                                         roc_auc_score(y2, r2[PERSIST]["p_te"]), roc_auc_score(y2, r2[BASE]["p_te"]),
                                                         roc_auc_score(y2, r2["Contexto (IPC, tipo de cambio, sistema)"]["p_te"])))

    # -------------------------------------------------- errores del modelo base
    P("\n== ERRORES DEL MODELO BASE EN PRUEBA (umbral %.2f) ==" % UMBRAL)
    pb = res[BASE]["p_te"]
    pred = (pb >= UMBRAL).astype(int)
    err = te.assign(p=pb, pred=pred, y=y_te)[pred != y_te]
    for f, r in err.iterrows():
        P("%s: brecha(t) %+.2f -> brecha(t+1) %+.2f; etiqueta %d, predicho %d (p = %.2f)" % (
            mes(f), r.brecha_real_pp, r.brecha_mes_siguiente_pp, int(r.y), int(r.pred), r.p))
    sign_change = ((te.brecha_real_pp < 0) != (te.brecha_mes_siguiente_pp < 0))
    P("meses de prueba con cambio de signo entre t y t+1: %d; de ellos el modelo base acierta %d" % (
        sign_change.sum(), int((pred[sign_change.values] == y_te[sign_change.values]).sum())))
    P("persistencia: errores en prueba %d, todos cambios de signo por definición" % int((res[PERSIST]["clase"] != y_te).sum()))

    # ---------------------------------------------------------------- Figura 7
    fig, ax = plt.subplots(figsize=(6.5, 3.3))
    filas = [MAYOR, "Contexto (IPC, tipo de cambio, sistema)", PERSIST, "Inercia (brecha t, t−1, t−3)", BASE]
    etiquetas = {MAYOR: "Clase mayoritaria", "Contexto (IPC, tipo de cambio, sistema)": "Solo contexto\n(IPC, tipo de cambio, sistema)",
                 PERSIST: "Regla de persistencia\n(signo de la brecha actual)", "Inercia (brecha t, t−1, t−3)": "Solo inercia\n(brecha t, t−1, t−3)",
                 BASE: "Modelo base\n(inercia + contexto)"}
    ys = np.arange(len(filas))
    for y_, k in zip(ys, filas):
        c = AZUL if k == BASE else (NARANJA if k == PERSIST else GRIS)
        ax.plot(ci[k], [y_, y_], color=c, linewidth=2.2, solid_capstyle="round", zorder=2)
        ax.scatter([tabla[k]["auc_te"]], [y_], s=46, color=c, edgecolor="white", linewidth=1.2, zorder=3)
        ax.text(max(ci[k][1], tabla[k]["auc_te"]) + 0.012, y_, "%.2f" % tabla[k]["auc_te"], va="center", fontsize=8.5, color=TINTA)
    tope = len(filas) - 0.5
    ax.plot([0.5, 0.5], [-0.6, tope], color=TINTA2, linewidth=1)
    ax.plot([0.7, 0.7], [-0.6, tope], color=GRIS, linewidth=1, linestyle=(0, (4, 3)))
    ax.text(0.7, tope + 0.1, "meta (AUC > 0.70)", ha="center", va="bottom", fontsize=7.5, color=TINTA2)
    ax.text(0.5, tope + 0.1, "sin señal (0.50)", ha="center", va="bottom", fontsize=7.5, color=TINTA2)
    ax.set_yticks(ys)
    ax.set_yticklabels([etiquetas[k] for k in filas], fontsize=8)
    ax.set_xlim(0.2, 1.02)
    ax.set_ylim(-0.6, len(filas) + 0.4)
    ax.set_xlabel("AUC en el tramo de prueba (%s a %s, %d meses)" % (mes(te.index.min()), mes(te.index.max()), len(te)))
    ax.grid(axis="x", color=GRIS_CLARO, linewidth=0.8)
    ax.set_axisbelow(True)
    fig.savefig(os.path.join(salida, "fig7_auc_modelos.png"), dpi=220, bbox_inches="tight", facecolor="white")
    plt.close(fig)

    # ---------------------------------------------------------------- Figura 8
    fig, ax = plt.subplots(figsize=(6.5, 3.0))
    ax.axhline(UMBRAL, color=GRIS, linewidth=1, linestyle=(0, (4, 3)))
    ax.plot(te.index, pb, color=AZUL, linewidth=1.8, zorder=2)
    pos = y_te == 1
    ax.scatter(te.index[pos], np.full(pos.sum(), 1.07), s=22, color=NARANJA, marker="s", linewidth=0, label="Bajo rendimiento real (%d)" % pos.sum(), zorder=3)
    ax.scatter(te.index[~pos], np.full((~pos).sum(), 1.07), s=22, color=GRIS, marker="o", linewidth=0, label="No bajo rendimiento real (%d)" % (~pos).sum(), zorder=3)
    ax.scatter(err.index, err.p, s=60, facecolor="none", edgecolor=TINTA, linewidth=1.3, zorder=4, label="Error del modelo (%d)" % len(err))
    ax.set_ylim(-0.04, 1.16)
    ax.set_yticks([0, 0.25, 0.5, 0.75, 1.0])
    ax.set_ylabel("Probabilidad estimada de\nbajo rendimiento (mes siguiente)")
    ax.grid(axis="y", color=GRIS_CLARO, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=3, fontsize=7.5, handletextpad=0.3, columnspacing=1.2)
    fig.savefig(os.path.join(salida, "fig8_probabilidad_prueba.png"), dpi=220, bbox_inches="tight", facecolor="white")
    plt.close(fig)

    import sklearn
    P("\nversiones: pandas %s, numpy %s, scikit-learn %s" % (pd.__version__, np.__version__, sklearn.__version__))
    open(os.path.join(salida, "cifras_4_2.txt"), "w", encoding="utf-8").write("\n".join(out) + "\n")
    print("\n".join(out))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
