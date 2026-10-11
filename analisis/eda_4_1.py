# -*- coding: utf-8 -*-
"""4.1 Análisis exploratorio de datos: genera las figuras y las cifras que cita el informe.

Uso:
    python3 eda_4_1.py <carpeta_con_los_CSV> <carpeta_de_salida>

Lee (de la zona procesada exportada a CSV):
    tabla_analitica_mensual.csv, sbs_indicadores_adicionales.csv
Escribe:
    fig2_brecha_mensual.png, fig3_brecha_vs_afp.png, fig4_persistencia.png,
    fig5_auc_entrenamiento.png, fig6_traspasos.png, cifras_4_1.txt

Reglas:
  * Solo datos limpios (zona procesada). Los datos replicados de 3.7 no se usan.
  * Descripción sobre los 140 meses; lo que sirve para elegir variables (Figura 5)
    se calcula SOLO con el tramo de entrenamiento (hasta CORTE).
  * El mes sin etiqueta (agosto de 2026) no entra en nada que use la etiqueta.
"""
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu

CORTE = pd.Timestamp("2022-12-01")      # último mes de entrenamiento (propuesto)
OBJ = "objetivo_bajo_rendimiento_mes_siguiente"

# Paleta (blue/orange validados; gris solo como neutro de contexto)
AZUL, NARANJA = "#2a78d6", "#eb6834"
GRIS, GRIS_CLARO, TINTA, TINTA2 = "#898781", "#e1e0d9", "#0b0b0b", "#52514e"

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 8.5, "axes.edgecolor": GRIS_CLARO,
    "axes.labelcolor": TINTA2, "xtick.color": TINTA2, "ytick.color": TINTA2,
    "axes.spines.top": False, "axes.spines.right": False, "figure.dpi": 100,
    "axes.titlesize": 9, "legend.frameon": False,
})
MESES = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]


def mes(ts):
    return "%s-%d" % (MESES[ts.month - 1], ts.year)


def limpiar(ax, grid="y"):
    ax.grid(axis=grid, color=GRIS_CLARO, linewidth=0.8)
    ax.set_axisbelow(True)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRIS_CLARO)


def guardar(fig, ruta):
    fig.savefig(ruta, dpi=220, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def auc_dir(x, y):
    """AUC de Mann-Whitney: P(valor del mes con bajo rendimiento > valor del mes sin él)."""
    d = pd.concat([x, y], axis=1).dropna()
    d.columns = ["x", "y"]
    pos, neg = d.x[d.y == 1], d.x[d.y == 0]
    return mannwhitneyu(pos, neg).statistic / (len(pos) * len(neg)), len(d)


def main(carpeta, salida):
    os.makedirs(salida, exist_ok=True)
    t = pd.read_csv(os.path.join(carpeta, "tabla_analitica_mensual.csv"), parse_dates=["fecha_mes"]).set_index("fecha_mes")
    ad = pd.read_csv(os.path.join(carpeta, "sbs_indicadores_adicionales.csv"), parse_dates=["fecha"])
    out = []
    P = lambda s="": out.append(s)

    b = t.brecha_real_pp
    lab = t.dropna(subset=[OBJ])
    tr, te = lab[lab.index <= CORTE], lab[lab.index > CORTE]

    # ------------------------------------------------------------------ datos
    P("== DATOS ==")
    P("filas %d, %s a %s" % (len(t), mes(t.index.min()), mes(t.index.max())))
    P("objetivo: positivos %d, negativos %d, sin etiqueta %d" % (
        (t[OBJ] == 1).sum(), (t[OBJ] == 0).sum(), t[OBJ].isna().sum()))
    P("entrenamiento (<= %s): %d meses, %d positivos (%.1f %%)" % (mes(CORTE), len(tr), tr[OBJ].sum(), 100 * tr[OBJ].mean()))
    P("prueba (> %s): %d meses, %d positivos (%.1f %%)" % (mes(CORTE), len(te), te[OBJ].sum(), 100 * te[OBJ].mean()))

    # ------------------------------------------------------- Figura 2: brecha
    P("\n== FIGURA 2: brecha mensual (Integra menos promedio simple de las otras tres) ==")
    P("media %.3f, mediana %.3f, desv. est. %.3f" % (b.mean(), b.median(), b.std()))
    P("meses por debajo de 0: %d de %d (%.1f %%); en 0 o por encima: %d" % ((b < 0).sum(), len(b), 100 * (b < 0).mean(), (b >= 0).sum()))
    P("mínimo %.3f (%s); máximo %.3f (%s); último mes %.3f (%s)" % (b.min(), mes(b.idxmin()), b.max(), mes(b.idxmax()), b.iloc[-1], mes(b.index[-1])))
    rachas, cur, ini = [], 0, None
    for d, v in (b < 0).items():
        if v:
            if cur == 0:
                ini = d
            cur += 1
        elif cur:
            rachas.append((cur, ini, d - pd.offsets.MonthBegin(1)))
            cur = 0
    if cur:
        rachas.append((cur, ini, b.index[-1]))
    rachas.sort(key=lambda r: -r[0])
    for n, a, z in rachas[:5]:
        P("racha bajo cero: %d meses (%s a %s)" % (n, mes(a), mes(z)))
    fin_ok = 0
    for v_ in b.values[::-1]:
        if v_ >= 0:
            fin_ok += 1
        else:
            break
    P("meses consecutivos en 0 o por encima al cierre de la serie: %d" % fin_ok)
    por_anio = b.groupby(b.index.year).agg(meses="size", bajo=lambda s: int((s < 0).sum()), media="mean")
    por_anio["pct_bajo"] = (100 * por_anio.bajo / por_anio.meses).round(1)
    P(por_anio.round(2).to_string())

    fig, ax = plt.subplots(figsize=(6.5, 3.0))
    ancho = 24
    col = np.where(b.values < 0, NARANJA, AZUL)
    ax.bar(b.index, b.values, width=ancho, color=col, linewidth=0)
    ax.axhline(0, color=TINTA2, linewidth=1)
    limpiar(ax)
    ax.set_ylabel("Brecha de rentabilidad real a 12 meses (pp)")
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.set_xlim(pd.Timestamp("2014-10-01"), pd.Timestamp("2026-12-01"))
    ax.set_ylim(-3.6, 3.0)
    # etiquetas selectivas
    ax.annotate("mín. −%.2f\n%s" % (abs(b.min()), mes(b.idxmin())), (b.idxmin(), b.min()), xytext=(0, -3), textcoords="offset points",
                ha="center", va="top", fontsize=7.5, color=TINTA2)
    ax.annotate("máx. +%.2f\n%s" % (b.max(), mes(b.idxmax())), (b.idxmax(), b.max()), xytext=(0, 3), textcoords="offset points",
                ha="center", va="bottom", fontsize=7.5, color=TINTA2)
    n, a, z = rachas[0]
    y0 = 1.55
    ax.annotate("", xy=(z, y0), xytext=(a, y0), arrowprops=dict(arrowstyle="-", color=TINTA2, lw=1))
    ax.text(a + (z - a) / 2, y0 + 0.08, "%d meses seguidos\npor debajo de 0" % n, ha="center", va="bottom", fontsize=7.5, color=TINTA2)
    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(color=NARANJA, label="Integra por debajo del promedio (%d meses)" % (b < 0).sum()),
                       Patch(color=AZUL, label="Integra en el promedio o por encima (%d meses)" % (b >= 0).sum())],
              loc="lower left", fontsize=7.5, ncol=1, bbox_to_anchor=(0.0, 0.0))
    guardar(fig, os.path.join(salida, "fig2_brecha_mensual.png"))

    # ------------------------------------------- Figura 3: frente a cada rival
    P("\n== FIGURA 3: Integra menos cada AFP ==")
    I = t.rent_real_integra_12m
    rivales = [("habitat", "Habitat"), ("prima", "Prima"), ("profuturo", "Profuturo")]
    difs = {}
    for k, nombre in rivales:
        d = I - t["rent_real_%s_12m" % k]
        difs[nombre] = d
        P("Integra - %s: media %.3f, mediana %.3f, meses por debajo %d de %d (%.1f %%)" % (nombre, d.mean(), d.median(), (d < 0).sum(), len(d), 100 * (d < 0).mean()))
    cuatro = t[["rent_real_integra_12m", "rent_real_habitat_12m", "rent_real_prima_12m", "rent_real_profuturo_12m"]]
    P("Habitat es la de mayor rentabilidad entre las cuatro en %d de %d meses (%.1f %%)" % (
        (t.rent_real_habitat_12m == cuatro.max(axis=1)).sum(), len(t), 100 * (t.rent_real_habitat_12m == cuatro.max(axis=1)).mean()))
    P("Integra es la última de las cuatro en %d meses" % (cuatro.rank(axis=1, ascending=False).rent_real_integra_12m == 4).sum())
    P("rentabilidad real media a 12 meses: " + ", ".join("%s %.2f" % (n, t["rent_real_%s_12m" % k].mean()) for k, n in [("integra", "Integra")] + rivales))
    sup = (I > t.rent_real_prima_12m) & (I > t.rent_real_profuturo_12m)
    P("de los %d meses con brecha < 0, en %d Integra supera a Prima y a Profuturo" % ((b < 0).sum(), ((b < 0) & sup).sum()))

    fig, axs = plt.subplots(3, 1, figsize=(6.5, 6.0), sharex=True, sharey=True)
    for ax, (k, nombre) in zip(axs, rivales):
        d = difs[nombre]
        c = NARANJA if nombre == "Habitat" else GRIS
        ax.axhline(0, color=TINTA2, linewidth=1)
        ax.fill_between(d.index, 0, d.values, where=d.values < 0, color=c, alpha=0.12, linewidth=0)
        ax.plot(d.index, d.values, color=c, linewidth=1.6, solid_capstyle="round")
        limpiar(ax)
        ax.set_ylabel("pp")
        ax.text(0.0, 1.04, "Integra menos %s" % nombre, transform=ax.transAxes, va="bottom", fontsize=8.5, fontweight="bold", color=TINTA)
        ax.text(1.0, 1.04, "media %s pp · por debajo en %d de %d meses (%.1f %%)" % (
            ("%+.2f" % d.mean()).replace("-", "−"), (d < 0).sum(), len(d), 100 * (d < 0).mean()),
                transform=ax.transAxes, va="bottom", ha="right", fontsize=7.8, color=TINTA2)
    axs[-1].xaxis.set_major_locator(mdates.YearLocator())
    axs[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    axs[-1].set_xlim(pd.Timestamp("2014-10-01"), pd.Timestamp("2026-12-01"))
    axs[0].set_ylim(-6.0, 6.5)
    fig.align_ylabels(axs)
    fig.tight_layout(h_pad=1.4)
    guardar(fig, os.path.join(salida, "fig3_brecha_vs_afp.png"))

    # ---------------------------------------------- Figura 4: persistencia
    P("\n== FIGURA 4: persistencia de la brecha ==")
    v = t.dropna(subset=["brecha_mes_siguiente_pp"])
    x, y = v.brecha_real_pp, v.brecha_mes_siguiente_pp
    r = np.corrcoef(x, y)[0, 1]
    rs = x.corr(y, method="spearman")
    igual = (x < 0) == (y < 0)
    P("pares (t, t+1): %d; Pearson %.3f; Spearman %.3f" % (len(v), r, rs))
    cx = pd.crosstab(x < 0, y < 0)
    P("signo en t -> t+1: sigue por debajo %d; sigue por encima %d; pasa a por encima %d; pasa a por debajo %d" % (
        cx.loc[True, True], cx.loc[False, False], cx.loc[True, False], cx.loc[False, True]))
    P("acuerdo de signo: %d de %d (%.1f %%); cambios de signo: %d" % (igual.sum(), len(v), 100 * igual.mean(), (~igual).sum()))
    P("P(por debajo en t+1 | por debajo en t) = %.3f; P(por debajo en t+1 | no por debajo en t) = %.3f" % (
        ((y < 0) & (x < 0)).sum() / (x < 0).sum(), ((y < 0) & (x >= 0)).sum() / (x >= 0).sum()))
    P("|brecha en t| mediana: %.3f en meses con cambio de signo vs %.3f sin cambio" % (x[~igual].abs().median(), x[igual].abs().median()))
    for nom, df in (("entrenamiento", tr), ("prueba", te)):
        xx, yy = df.brecha_real_pp, df.brecha_mes_siguiente_pp
        P("acuerdo de signo en %s: %d de %d (%.1f %%)" % (nom, ((xx < 0) == (yy < 0)).sum(), len(df), 100 * ((xx < 0) == (yy < 0)).mean()))
    auc_b, _ = auc_dir(-lab.brecha_real_pp, lab[OBJ])
    P("AUC de la regla 'menor brecha hoy -> más riesgo mañana' (todos los meses con etiqueta): %.3f" % auc_b)
    P("proporción de meses con etiqueta = 1: %.1f %% (referencia de mayoría)" % (100 * lab[OBJ].mean()))
    P("autocorrelación de la brecha, rezagos 1, 3, 6, 12: " + ", ".join("%.3f" % b.autocorr(k) for k in (1, 3, 6, 12)))

    fig, ax = plt.subplots(figsize=(5.2, 5.0))
    lim = 3.3
    ax.plot([-lim, lim], [-lim, lim], color=GRIS_CLARO, linewidth=1, zorder=1)
    ax.axhline(0, color=TINTA2, linewidth=1, zorder=1)
    ax.axvline(0, color=TINTA2, linewidth=1, zorder=1)
    ax.scatter(x[igual], y[igual], s=24, color=AZUL, alpha=0.8, edgecolor="white", linewidth=0.6, label="Mantiene el signo (%d)" % igual.sum(), zorder=3)
    ax.scatter(x[~igual], y[~igual], s=30, color=NARANJA, alpha=0.95, edgecolor="white", linewidth=0.6, marker="D", label="Cambia de signo (%d)" % (~igual).sum(), zorder=4)
    ax.set_xlim(-lim, lim)
    ax.set_ylim(-lim, lim)
    ax.set_aspect("equal")
    ax.set_xlabel("Brecha del mes t (pp)")
    ax.set_ylabel("Brecha del mes t+1 (pp)")
    limpiar(ax, grid="both")
    for (xx, yy, txt, ha, va) in [(-lim + 0.1, -lim + 0.15, "Sigue por debajo: %d" % cx.loc[True, True], "left", "bottom"),
                                  (lim - 0.1, lim - 0.15, "Sigue por encima: %d" % cx.loc[False, False], "right", "top"),
                                  (-lim + 0.1, lim - 0.15, "Pasa a por encima: %d" % cx.loc[True, False], "left", "top"),
                                  (lim - 0.1, -lim + 0.15, "Pasa a por debajo: %d" % cx.loc[False, True], "right", "bottom")]:
        ax.text(xx, yy, txt, ha=ha, va=va, fontsize=8, color=TINTA2)
    ax.text(0.5, 1.02, "r = %.2f (n = %d pares de meses consecutivos)" % (r, len(v)), transform=ax.transAxes, ha="center", va="bottom", fontsize=8.5, color=TINTA)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=2, fontsize=8)
    guardar(fig, os.path.join(salida, "fig4_persistencia.png"))

    # -------------------------------------- Figura 5: AUC en entrenamiento
    P("\n== FIGURA 5: AUC univariado, solo entrenamiento (<= %s) ==" % mes(CORTE))
    cand = [("brecha_real_pp", "Brecha del mes t"),
            ("brecha_lag1", "Brecha del mes t−1"),
            ("brecha_lag3", "Brecha del mes t−3"),
            ("tipo_cambio_lag1", "Tipo de cambio (nivel, mes t−1)"),
            ("ipc_lag1", "IPC Lima, var. mensual (mes t−1)"),
            ("var_tc_12m_pct_lag1", "Tipo de cambio, var. a 12 meses (mes t−1)"),
            ("sistema_lag1", "Rentab. real del sistema (mes t−1)")]
    rng = np.random.default_rng(7)
    filas = []
    for col_, nombre in cand:
        a, n = auc_dir(tr[col_], tr[OBJ])
        dd = pd.concat([tr[col_], tr[OBJ]], axis=1).dropna().values
        bs = []
        for _ in range(500):
            i = rng.integers(0, len(dd), len(dd))
            s = dd[i]
            if s[:, 1].min() == s[:, 1].max():
                continue
            p_, q_ = s[s[:, 1] == 1, 0], s[s[:, 1] == 0, 0]
            bs.append(mannwhitneyu(p_, q_).statistic / (len(p_) * len(q_)))
        lo, hi = np.percentile(bs, [5, 95])
        fuerza = max(a, 1 - a)
        if a >= 0.5:
            flo, fhi = lo, hi
        else:
            flo, fhi = 1 - hi, 1 - lo
        filas.append(dict(var=col_, nombre=nombre, n=n, auc=a, fuerza=fuerza, flo=max(flo, 0.5) if flo < 0.5 < fhi else flo, fhi=fhi,
                          mas_riesgo=a >= 0.5))
    df5 = pd.DataFrame(filas).sort_values("fuerza")
    for _, f in df5.sort_values("fuerza", ascending=False).iterrows():
        P("%-22s n=%3d AUC=%.3f -> fuerza %.3f (%s; intervalo bootstrap 5-95 %%: %.3f a %.3f)" % (
            f["var"], f.n, f.auc, f.fuerza, "valor alto = más riesgo" if f.mas_riesgo else "valor alto = menos riesgo", f.flo, f.fhi))
    tc = tr.tipo_cambio_soles_usd
    P("tipo de cambio en entrenamiento: %.3f (%s) a %.3f (%s); correlación de rangos con el tiempo: %.3f" % (
        tc.iloc[0], mes(tc.index[0]), tc.iloc[-1], mes(tc.index[-1]), pd.Series(tc.values).corr(pd.Series(np.arange(len(tc))), method="spearman")))
    P("tipo de cambio en toda la ventana: mín. %.2f, máx. %.2f" % (t.tipo_cambio_soles_usd.min(), t.tipo_cambio_soles_usd.max()))
    P("meses de entrenamiento con texto de memorias: %d; ejercicios distintos: %d" % (
        tr.anio_ejercicio.notna().sum(), tr.anio_ejercicio.nunique()))

    fig, ax = plt.subplots(figsize=(6.5, 3.4))
    ys = np.arange(len(df5))
    cols = [NARANJA if m else AZUL for m in df5.mas_riesgo]
    ax.barh(ys, df5.fuerza - 0.5, left=0.5, color=cols, height=0.55, linewidth=0)
    ax.errorbar(df5.fuerza, ys, xerr=[df5.fuerza - df5.flo, df5.fhi - df5.fuerza], fmt="none", ecolor=TINTA2, elinewidth=1, capsize=2.5, zorder=4)
    for yy, f in zip(ys, df5.itertuples()):
        ax.text(max(f.fhi, f.fuerza) + 0.012, yy, "%.2f" % f.fuerza, va="center", fontsize=8, color=TINTA)
    ax.set_yticks(ys)
    ax.set_yticklabels(df5.nombre, fontsize=8)
    tope = len(df5) - 0.45
    ax.plot([0.5, 0.5], [-0.6, tope], color=TINTA2, linewidth=1)
    ax.plot([0.7, 0.7], [-0.6, tope], color=GRIS, linewidth=1, linestyle=(0, (4, 3)))
    ax.text(0.7, tope + 0.12, "meta de 4.2 (AUC > 0.70)", ha="center", va="bottom", fontsize=7.5, color=TINTA2)
    ax.text(0.5, tope + 0.12, "sin señal (0.50)", ha="center", va="bottom", fontsize=7.5, color=TINTA2)
    ax.set_xlim(0.45, 1.0)
    ax.set_ylim(-0.6, len(df5) + 0.5)
    ax.set_xlabel("Capacidad de separar meses con y sin bajo rendimiento (AUC, entrenamiento)")
    limpiar(ax, grid="x")
    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(color=NARANJA, label="Valor alto → más riesgo de bajo rendimiento"),
                       Patch(color=AZUL, label="Valor alto → menos riesgo")],
              loc="lower right", fontsize=7.5)
    guardar(fig, os.path.join(salida, "fig5_auc_entrenamiento.png"))

    # ------------------------------------------- Figura 6: traspasos netos
    P("\n== FIGURA 6: traspasos netos de Integra frente a la brecha ==")
    tn = ad[(ad.indicador == "traspasos_afiliados_neto") & (ad.etiqueta_original == "Integra")].copy()
    tn["fecha"] = tn.fecha.dt.to_period("M").dt.to_timestamp()
    s = tn.drop_duplicates("fecha").set_index("fecha").valor.sort_index()
    s = s[(s.index >= t.index.min()) & (s.index <= t.index.max())]
    P("traspasos netos de Integra: %d meses, %s a %s, unidad: afiliados" % (len(s), mes(s.index.min()), mes(s.index.max())))
    j = pd.concat([b, s], axis=1, keys=["brecha", "tras"]).dropna()
    lags = [0, 3, 6, 9, 12]
    pe = [j.brecha.corr(j.tras.shift(-k)) for k in lags]
    sp = [j.brecha.corr(j.tras.shift(-k), method="spearman") for k in lags]
    P("correlación mensual brecha(t) vs traspaso neto(t+k), k=%s: Pearson %s; Spearman %s" % (
        lags, [round(float(q), 3) for q in pe], [round(float(q), 3) for q in sp]))
    P("máximo |Pearson| = %.3f; máximo |Spearman| = %.3f" % (np.max(np.abs(pe)), np.max(np.abs(sp))))
    anual = pd.concat([j.brecha.groupby(j.index.year).mean(), j.tras.groupby(j.index.year).sum(), j.tras.groupby(j.index.year).size()],
                      axis=1, keys=["brecha_media", "traspaso_neto", "meses"])
    P(anual.round(2).to_string())
    af = t.afiliados_integra.dropna()
    P("afiliados de Integra (tabla analítica): %s = %s; %s = %s; variación %+.1f %%; último dato %s" % (
        mes(af.index.min()), "{:,.0f}".format(af.iloc[0]), mes(af.index.max()), "{:,.0f}".format(af.iloc[-1]),
        100 * (af.iloc[-1] / af.iloc[0] - 1), mes(af.index.max())))
    P("traspaso neto acumulado ene-2015 a abr-2026: %s afiliados (%.2f %% del aumento de la base)" % (
        "{:+,.0f}".format(j.tras.sum()), 100 * j.tras.sum() / (af.iloc[-1] - af.iloc[0])))
    dic25 = t.afiliados_integra.get(pd.Timestamp("2025-12-01"))
    P("traspaso neto de 2025: %s = %.2f %% de los afiliados de dic-2025 (%s)" % (
        "{:+,.0f}".format(anual.loc[2025, "traspaso_neto"]), 100 * abs(anual.loc[2025, "traspaso_neto"]) / dic25, "{:,.0f}".format(dic25)))
    P("años con traspaso neto positivo y brecha media negativa: %s" % list(anual.index[(anual.traspaso_neto > 0) & (anual.brecha_media < 0)]))

    fig, axs = plt.subplots(2, 1, figsize=(6.5, 4.6), sharex=True)
    xs = anual.index.values
    a1, a2 = axs
    a1.bar(xs, anual.brecha_media, color=np.where(anual.brecha_media < 0, NARANJA, AZUL), width=0.6, linewidth=0)
    a1.axhline(0, color=TINTA2, linewidth=1)
    a1.set_ylabel("Brecha media (pp)")
    a1.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v_, p_: ("%.1f" % v_).replace("-", "−")))
    a1.text(0.0, 1.03, "Brecha media del año", transform=a1.transAxes, fontsize=8.5, fontweight="bold", color=TINTA, va="bottom")
    a2.bar(xs, anual.traspaso_neto, color=np.where(anual.traspaso_neto < 0, NARANJA, AZUL), width=0.6, linewidth=0)
    a2.axhline(0, color=TINTA2, linewidth=1)
    a2.set_ylabel("Afiliados (neto)")
    a2.text(0.0, 1.03, "Traspasos netos de Integra en el año (entran − salen)", transform=a2.transAxes, fontsize=8.5, fontweight="bold", color=TINTA, va="bottom")
    a2.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v_, p_: "{:,.0f}".format(v_).replace("-", "−")))
    for xx, vv in zip(xs, anual.brecha_media):
        a1.annotate(("%+.2f" % vv).replace("-", "−"), (xx, vv), xytext=(0, 3 if vv >= 0 else -3), textcoords="offset points",
                    ha="center", va="bottom" if vv >= 0 else "top", fontsize=7.5, color=TINTA2)
    for xx, vv in zip(xs, anual.traspaso_neto):
        a2.annotate(("{:+,.0f}".format(vv)).replace("-", "−"), (xx, vv), xytext=(0, 3 if vv >= 0 else -3), textcoords="offset points",
                    ha="center", va="bottom" if vv >= 0 else "top", fontsize=7.0, color=TINTA2)
    a1.set_ylim(-1.95, 1.05)
    a2.set_ylim(-31000, 20500)
    for ax in axs:
        limpiar(ax)
    a2.set_xticks(xs)
    a2.set_xticklabels([str(q) if q != 2026 else "2026\n(ene–abr)" for q in xs])
    fig.align_ylabels(axs)
    fig.tight_layout(h_pad=1.8)
    guardar(fig, os.path.join(salida, "fig6_traspasos.png"))

    open(os.path.join(salida, "cifras_4_1.txt"), "w", encoding="utf-8").write("\n".join(out) + "\n")
    print("\n".join(out))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
