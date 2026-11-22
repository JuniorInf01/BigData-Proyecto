#!/usr/bin/env bash
# Prepara gold para el dashboard: ubicación de resultados del workgroup (Quick Sight la necesita),
# tabla de predicciones del modelo y una vista plana del modelo estrella.
set -euo pipefail
export AWS_PAGER=""
ACC=672469410615
GOLD=s3://afp-integra-gold-$ACC
OUT=$GOLD/_athena/

aws athena update-work-group --work-group primary \
  --configuration-updates "ResultConfigurationUpdates={OutputLocation=$OUT}"
echo "OK  workgroup primary -> $OUT"

run() {
  local id s
  id=$(aws athena start-query-execution --query-string "$1" --work-group primary --query QueryExecutionId --output text)
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

run "DROP TABLE IF EXISTS afp_gold.predicciones_modelo"
run "CREATE EXTERNAL TABLE afp_gold.predicciones_modelo (
  fecha_mes date, tramo string, prob_bajo_rendimiento double, prediccion int,
  real double, prob_persistencia double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '$GOLD/modelo/predicciones/'
TBLPROPERTIES ('skip.header.line.count'='1')"
echo "OK  afp_gold.predicciones_modelo"

run "CREATE OR REPLACE VIEW afp_gold.v_rendimiento AS
SELECT t.fecha_mes, t.anio, t.trimestre, t.mes,
       a.afp, a.tipo_entidad, a.es_afp_objetivo,
       i.codigo AS indicador, i.descripcion AS indicador_descripcion, i.unidad,
       h.valor, h.fuente
FROM afp_gold.hecho_rendimiento_mensual h
JOIN afp_gold.dim_tiempo t    ON t.id_tiempo = h.id_tiempo
JOIN afp_gold.dim_afp a       ON a.id_afp = h.id_afp
JOIN afp_gold.dim_indicador i ON i.id_indicador = h.id_indicador"
echo "OK  afp_gold.v_rendimiento"

echo; echo "== Control (esperado: v_rendimiento 2484; predicciones 43 prueba + 1 sin_etiqueta) =="
show "SELECT 'v_rendimiento' AS objeto, count(*) AS filas FROM afp_gold.v_rendimiento
      UNION ALL SELECT 'pred_' || tramo, count(*) FROM afp_gold.predicciones_modelo GROUP BY tramo"
