#!/usr/bin/env bash
# Zona GOLD — modelo estrella en Athena (CTAS -> Parquet en s3://afp-integra-gold-*)
#   hecho_rendimiento_mensual  (grano: mes x AFP x indicador)
#   dim_tiempo | dim_afp | dim_indicador
# Fuente: base afp_silver del Glue Data Catalog. Re-ejecutable (borra y recrea).
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

show() {  # ejecuta y muestra resultado como tabla
  run "$1"
  aws athena get-query-results --query-execution-id "$QID" \
    --query 'ResultSet.Rows[].Data[].VarCharValue' --output text | sed 's/\t/ | /g'
}

ctas() {  # $1 tabla  $2 propiedades extra  $3 SELECT
  aws s3 rm "$GOLD/$1/" --recursive --quiet || true
  run "DROP TABLE IF EXISTS afp_gold.$1"
  run "CREATE TABLE afp_gold.$1 WITH (format='PARQUET', write_compression='SNAPPY', external_location='$GOLD/$1/' $2) AS $3"
  echo "OK  afp_gold.$1"
}

run "CREATE DATABASE IF NOT EXISTS afp_gold"

# ---------------- dimensiones
ctas dim_tiempo "" "
SELECT year(fecha_mes)*100 + month(fecha_mes) AS id_tiempo,
       fecha_mes, year(fecha_mes) AS anio, quarter(fecha_mes) AS trimestre, month(fecha_mes) AS mes
FROM afp_silver.tabla_analitica_mensual"

ctas dim_afp "" "
SELECT * FROM (VALUES
  (0, 'Sistema',   'Promedio del sistema', false),
  (1, 'Habitat',   'AFP',                  false),
  (2, 'Integra',   'AFP',                  true),
  (3, 'Prima',     'AFP',                  false),
  (4, 'Profuturo', 'AFP',                  false)
) AS t(id_afp, afp, tipo_entidad, es_afp_objetivo)"

ctas dim_indicador "" "
SELECT * FROM (VALUES
  (1, 'rent_real_12m',          'Rentabilidad real últimos 12 meses, Fondo 2',    '%',         'BCRP JSON (dic-2018 SBS)'),
  (2, 'rent_nominal_12m',       'Rentabilidad nominal últimos 12 meses, Fondo 2', '%',         'SBS'),
  (3, 'afiliados_activos',      'Afiliados activos',                              'afiliados', 'SBS'),
  (4, 'valor_cuota_fin_mes_f2', 'Valor cuota del último día del mes, Fondo 2',    'S/',        'SBS')
) AS t(id_indicador, codigo, descripcion, unidad, fuente)"

# ---------------- hecho
ctas hecho_rendimiento_mensual ", partitioned_by=ARRAY['anio']" "
WITH base AS (
  SELECT fecha_mes, u.afp, 1 AS id_indicador, u.valor, fuente_sistema AS fuente
  FROM afp_silver.tabla_analitica_mensual
  CROSS JOIN UNNEST(
     ARRAY['Sistema','Habitat','Integra','Prima','Profuturo'],
     ARRAY[rent_real_sistema_12m, rent_real_habitat_12m, rent_real_integra_12m,
           rent_real_prima_12m, rent_real_profuturo_12m]) AS u(afp, valor)
  UNION ALL
  SELECT fecha_mes, afp, 2, valor, 'SBS' FROM afp_silver.sbs_rentabilidad
  WHERE tipo_rentabilidad = 'nominal_12m'
  UNION ALL
  SELECT fecha_mes, afp, 3, afiliados, 'SBS' FROM afp_silver.sbs_afiliados
  UNION ALL
  SELECT fecha_mes, afp, 4, max_by(valor_cuota, fecha), 'SBS' FROM afp_silver.sbs_valor_cuota
  WHERE fondo = 2 GROUP BY fecha_mes, afp
)
SELECT t.id_tiempo, a.id_afp, b.id_indicador, b.valor, b.fuente, t.anio
FROM base b
JOIN afp_gold.dim_tiempo t ON t.fecha_mes = b.fecha_mes
JOIN afp_gold.dim_afp    a ON a.afp       = b.afp
WHERE b.valor IS NOT NULL"

# ---------------- validación
echo; echo "== Filas por indicador (esperado: 700 / 680 / 544 / 560 = 2484) =="
show "SELECT i.codigo, count(*) AS filas FROM afp_gold.hecho_rendimiento_mensual h
      JOIN afp_gold.dim_indicador i ON i.id_indicador = h.id_indicador GROUP BY i.codigo ORDER BY i.codigo"

echo; echo "== Duplicados de grano (esperado 0) =="
show "SELECT count(*) AS duplicados FROM (SELECT id_tiempo, id_afp, id_indicador
      FROM afp_gold.hecho_rendimiento_mensual GROUP BY 1,2,3 HAVING count(*) > 1)"

echo; echo "== Consulta analítica: brecha real Integra vs promedio otras 3, por año =="
show "SELECT t.anio,
        round(avg(CASE WHEN a.afp='Integra' THEN h.valor END)
            - avg(CASE WHEN a.afp IN ('Habitat','Prima','Profuturo') THEN h.valor END), 3) AS brecha_pp
      FROM afp_gold.hecho_rendimiento_mensual h
      JOIN afp_gold.dim_tiempo t    ON t.id_tiempo = h.id_tiempo
      JOIN afp_gold.dim_afp a       ON a.id_afp = h.id_afp
      JOIN afp_gold.dim_indicador i ON i.id_indicador = h.id_indicador
      WHERE i.codigo = 'rent_real_12m'
      GROUP BY t.anio ORDER BY t.anio"
