#!/usr/bin/env bash
# GOLD — dataset para el modelo con objetivo SIN ventanas solapadas.
# Rentabilidad mensual = variación del valor cuota de fin de mes (Fondo 2).
# Objetivo: 1 si el mes siguiente Integra rinde menos que el promedio de Habitat/Prima/Profuturo.
set -euo pipefail
export AWS_PAGER=""
ACC=672469410615
GOLD=s3://afp-integra-gold-$ACC
OUT=$GOLD/_athena/

run() {
  local id s
  id=$(aws athena start-query-execution --query-string "$1" \
        --result-configuration OutputLocation=$OUT --query QueryExecutionId --output text)
  while :; do
    s=$(aws athena get-query-execution --query-execution-id "$id" --query QueryExecution.Status.State --output text)
    [ "$s" = SUCCEEDED ] && break
    if [ "$s" = FAILED ] || [ "$s" = CANCELLED ]; then
      echo "ERROR en: ${1:0:80}"
      aws athena get-query-execution --query-execution-id "$id" --query QueryExecution.Status.StateChangeReason --output text
      exit 1
    fi
    sleep 2
  done
  QID=$id
}
show() {
  run "$1"
  aws athena get-query-results --query-execution-id "$QID" \
    --query 'ResultSet.Rows[].Data[].VarCharValue' --output text | sed 's/\t/ | /g'
}

T=dataset_modelo_mensual
aws s3 rm "$GOLD/$T/" --recursive --quiet || true
run "DROP TABLE IF EXISTS afp_gold.$T"
run "CREATE TABLE afp_gold.$T WITH (format='PARQUET', write_compression='SNAPPY', external_location='$GOLD/$T/') AS
WITH vc AS (
  SELECT fecha_mes, afp, max_by(valor_cuota, fecha) AS vc
  FROM afp_silver.sbs_valor_cuota
  WHERE fondo = 2  -- solo meses completos: septiembre 2026 llega solo hasta el día 25
    AND fecha_mes <= (SELECT max(fecha_mes) FROM afp_silver.tabla_analitica_mensual)
  GROUP BY fecha_mes, afp
), r AS (
  SELECT fecha_mes, afp, 100 * (vc / lag(vc) OVER (PARTITION BY afp ORDER BY fecha_mes) - 1) AS r
  FROM vc
), w AS (
  SELECT fecha_mes,
         max(CASE WHEN afp='Integra'   THEN r END) AS r_mes_integra,
         max(CASE WHEN afp='Habitat'   THEN r END) AS r_mes_habitat,
         max(CASE WHEN afp='Prima'     THEN r END) AS r_mes_prima,
         max(CASE WHEN afp='Profuturo' THEN r END) AS r_mes_profuturo
  FROM r GROUP BY fecha_mes
), d AS (
  SELECT *, r_mes_integra - (r_mes_habitat + r_mes_prima + r_mes_profuturo) / 3 AS brecha_mensual_pp FROM w
), l AS (
  SELECT *,
         lag(brecha_mensual_pp, 1)  OVER (ORDER BY fecha_mes) AS brecha_mensual_lag1,
         lag(brecha_mensual_pp, 2)  OVER (ORDER BY fecha_mes) AS brecha_mensual_lag2,
         lag(brecha_mensual_pp, 3)  OVER (ORDER BY fecha_mes) AS brecha_mensual_lag3,
         lead(brecha_mensual_pp, 1) OVER (ORDER BY fecha_mes) AS brecha_mensual_siguiente_pp
  FROM d
)
SELECT t.fecha_mes, year(t.fecha_mes) AS anio,
       l.r_mes_integra, l.r_mes_habitat, l.r_mes_prima, l.r_mes_profuturo,
       l.brecha_mensual_pp, l.brecha_mensual_lag1, l.brecha_mensual_lag2, l.brecha_mensual_lag3,
       t.brecha_real_pp AS brecha_12m_pp, t.sistema_lag1,
       t.ipc_lag1, t.tipo_cambio_lag1, t.var_tc_12m_pct_lag1,
       t.anio_ejercicio AS memoria_ejercicio_usada,
       t.freq_renta_variable, t.freq_exterior, t.freq_volatilidad, t.freq_retiros,
       t.freq_pandemia_covid, t.freq_inflacion, t.freq_tipo_cambio, t.freq_tasa_interes,
       t.freq_alternativos, t.freq_sostenibilidad_esg, t.freq_traspasos, t.freq_comisiones,
       l.brecha_mensual_siguiente_pp,
       CASE WHEN l.brecha_mensual_siguiente_pp IS NULL THEN NULL
            WHEN l.brecha_mensual_siguiente_pp < 0 THEN 1 ELSE 0 END AS objetivo_bajo_rendimiento_mes_siguiente
FROM afp_silver.tabla_analitica_mensual t
JOIN l ON l.fecha_mes = t.fecha_mes"
echo "OK  afp_gold.$T"

echo; echo "== Filas y balance del objetivo (esperado: 140 filas; 0=67, 1=72, sin etiqueta=1) =="
show "SELECT coalesce(cast(objetivo_bajo_rendimiento_mes_siguiente AS varchar),'sin_etiqueta') AS objetivo, count(*) AS filas
      FROM afp_gold.$T GROUP BY 1 ORDER BY 1"

echo; echo "== Persistencia del signo (12m solapado vs mensual) =="
show "SELECT round(avg(CASE WHEN (brecha_12m_pp < 0) = (lead_12m < 0) THEN 1.0 ELSE 0 END), 3) AS persistencia_12m,
             round(avg(CASE WHEN (brecha_mensual_pp < 0) = (brecha_mensual_siguiente_pp < 0) THEN 1.0 ELSE 0 END), 3) AS persistencia_mensual
      FROM (SELECT *, lead(brecha_12m_pp) OVER (ORDER BY fecha_mes) AS lead_12m FROM afp_gold.$T)
      WHERE brecha_mensual_siguiente_pp IS NOT NULL"

echo; echo "== Exportación para el modelo =="
run "SELECT * FROM afp_gold.$T ORDER BY fecha_mes"
aws s3 cp "$OUT$QID.csv" "$HOME/dataset_modelo_mensual.csv" --quiet
echo "CSV descargado: ~/dataset_modelo_mensual.csv ($(wc -l < ~/dataset_modelo_mensual.csv) líneas con cabecera)"
