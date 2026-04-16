

#esta consulta crea una nueva tabla llamada `202508` en el dataset `bikes_cleaned` del proyecto `zephyrus-214513`, combinando los datos de las tablas `202508_1` y `202508_2` utilizando la cláusula `UNION ALL`.

create table `zephyrus-214513.bikes_cleaned.202508` as (select * from `zephyrus-214513.bikes_cleaned.202508_1` 
union all select * from `zephyrus-214513.bikes_cleaned.202508_2` 
)

#esta consulta calcula el tiempo promedio de viaje en minutos para cada mes, considerando los datos de los meses de abril a diciembre de 2025 y enero a marzo de 2026. Se agrupan los resultados por año y mes, y se ordenan cronológicamente.

#esta consulta fue descartada dado un pequeño error en la limpieza de datos ya que hubieron algunos archivos que contenien fechas incorrectas que fueron visualizadas una vez los datos fueron cargados en bigquery
SELECT 
  EXTRACT(YEAR FROM started_at) AS year,
  EXTRACT(MONTH FROM started_at) AS month,
  ROUND(AVG(TIMESTAMP_DIFF(ended_at, started_at, MINUTE))) AS avg_trip_minutes
FROM `zephyrus-214513.bikes_cleaned.2025*`
WHERE _TABLE_SUFFIX BETWEEN '04' AND '12'
GROUP BY 1,2

UNION ALL

SELECT 
  EXTRACT(YEAR FROM started_at),
  EXTRACT(MONTH FROM started_at),
  ROUND(AVG(TIMESTAMP_DIFF(ended_at, started_at, MINUTE)))
FROM `zephyrus-214513.bikes_cleaned.2026*`
WHERE _TABLE_SUFFIX BETWEEN '01' AND '03'
GROUP BY 1,2

ORDER BY 1,2;


#esta consulta calcula el tiempo promedio de viaje en minutos para cada día de la semana, considerando solo los viajes que duran 90 minutos o menos. Se agrupan los resultados por día de la semana y se ordenan en el orden de los días (domingo a sábado).

SELECT
  CASE EXTRACT(DAYOFWEEK FROM started_at)
    WHEN 1 THEN 'Domingo'
    WHEN 2 THEN 'Lunes'
    WHEN 3 THEN 'Martes'
    WHEN 4 THEN 'Miércoles'
    WHEN 5 THEN 'Jueves'
    WHEN 6 THEN 'Viernes'
    WHEN 7 THEN 'Sábado'
  END AS day_of_week,
  ROUND(AVG(TIMESTAMP_DIFF(ended_at, started_at, MINUTE))) AS avg_trip_minutes
FROM (
  SELECT * FROM `zephyrus-214513.bikes_cleaned.202504`
  WHERE EXTRACT(MONTH FROM started_at) = 4
  UNION ALL
  SELECT * FROM `zephyrus-214513.bikes_cleaned.202505`
  WHERE EXTRACT(MONTH FROM started_at) = 5
  UNION ALL
  SELECT * FROM `zephyrus-214513.bikes_cleaned.202506`
  WHERE EXTRACT(MONTH FROM started_at) = 6
  UNION ALL
  SELECT * FROM `zephyrus-214513.bikes_cleaned.202507`
  WHERE EXTRACT(MONTH FROM started_at) = 7
  UNION ALL
  SELECT * FROM `zephyrus-214513.bikes_cleaned.202508`
  WHERE EXTRACT(MONTH FROM started_at) = 8
  UNION ALL
  SELECT * FROM `zephyrus-214513.bikes_cleaned.202509`
  WHERE EXTRACT(MONTH FROM started_at) = 9
  UNION ALL
  SELECT * FROM `zephyrus-214513.bikes_cleaned.202510`
  WHERE EXTRACT(MONTH FROM started_at) = 10
  UNION ALL
  SELECT * FROM `zephyrus-214513.bikes_cleaned.202511`
  WHERE EXTRACT(MONTH FROM started_at) = 11
  UNION ALL
  SELECT * FROM `zephyrus-214513.bikes_cleaned.202512`
  WHERE EXTRACT(MONTH FROM started_at) = 12
  UNION ALL
  SELECT * FROM `zephyrus-214513.bikes_cleaned.202601`
  WHERE EXTRACT(MONTH FROM started_at) = 1
  UNION ALL
  SELECT * FROM `zephyrus-214513.bikes_cleaned.202602`
  WHERE EXTRACT(MONTH FROM started_at) = 2
  UNION ALL
  SELECT * FROM `zephyrus-214513.bikes_cleaned.202603`
  WHERE EXTRACT(MONTH FROM started_at) = 3
)
WHERE TIMESTAMP_DIFF(ended_at, started_at, MINUTE) <= 90
GROUP BY day_of_week
ORDER BY MIN(EXTRACT(DAYOFWEEK FROM started_at));


#esta consulta selecciona la calle principal de las estaciones de inicio de los viajes realizados por miembros, contando el número total de viajes para cada calle. Se ordenan por el número total de viajes en orden descendente.

SELECT
  main_street,
  COUNT(*) AS total_trips
FROM (
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)] AS main_street
  FROM `zephyrus-214513.bikes_cleaned.202504`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202505`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202506`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202507`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202508`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202509`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202510`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202511`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202512`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202601`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202602`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202603`
)
GROUP BY main_street
ORDER BY total_trips DESC;SELECT
  main_street,
  COUNT(*) AS total_trips
FROM (
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)] AS main_street
  FROM `zephyrus-214513.bikes_cleaned.202504`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202505`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202506`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202507`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202508`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202509`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202510`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202511`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202512`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202601`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202602`

  UNION ALL
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202603`
)
GROUP BY main_street
ORDER BY total_trips DESC;

#esta consulta selecciona la calle principal de las estaciones de inicio de los viajes realizados por miembros, contando el número total de viajes para cada calle. Se filtran las calles que tienen menos de 10 viajes y se ordenan por el número total de viajes en orden descendente.

SELECT
  main_street,
  COUNT(*) AS total_trips
FROM (
  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)] AS main_street
  FROM `zephyrus-214513.bikes_cleaned.202504`
  WHERE member_casual = 'member'

  UNION ALL

  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202505`
  WHERE member_casual = 'member'

  UNION ALL

  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202506`
  WHERE member_casual = 'member'

  UNION ALL

  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202507`
  WHERE member_casual = 'member'

  UNION ALL

  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202508`
  WHERE member_casual = 'member'

  UNION ALL

  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202509`
  WHERE member_casual = 'member'

  UNION ALL

  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202510`
  WHERE member_casual = 'member'

  UNION ALL

  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202511`
  WHERE member_casual = 'member'

  UNION ALL

  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202512`
  WHERE member_casual = 'member'

  UNION ALL

  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202601`
  WHERE member_casual = 'member'

  UNION ALL

  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202602`
  WHERE member_casual = 'member'

  UNION ALL

  SELECT
    SPLIT(REGEXP_REPLACE(start_station_name, r'^Public Rack - ', ''), ' & ')[OFFSET(0)]
  FROM `zephyrus-214513.bikes_cleaned.202603`
  WHERE member_casual = 'member'
)

GROUP BY main_street
HAVING COUNT(*) < 10
ORDER BY total_trips DESC;

