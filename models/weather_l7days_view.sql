{{ config(materialized='view') }}

with source_data as (

    SELECT city
        , PARSE_DATE('%Y-%m-%d',  date) AS dt
        , avg_temp
        , totalprecip_mm
        , avghumidity
        , SUM(avg_temp) OVER(PARTITION BY city ORDER BY PARSE_DATE('%Y-%m-%d',  date)) AS running_sum_avg_temp
        , SUM(totalprecip_mm) OVER(PARTITION BY city ORDER BY PARSE_DATE('%Y-%m-%d',  date)) AS running_sum_totalprecip_mm
        , CASE 
              WHEN (25 - avg_temp) <= 0
              THEN (25 - avg_temp) * -1 / 25
              ELSE (25 - avg_temp) / 25
          END AS diff_temp
        , CASE 
              WHEN (50 - avghumidity) < 0
              THEN (50 - avghumidity) * -1 / 30
              ELSE (50 - avghumidity) / 30
          END AS diff_humidity
          , totalprecip_mm / 50 AS diff_totalprecip
    FROM `advalyze-355820.weather.weather_l7days` 

)

select city
     , dt as date
     , running_sum_avg_temp
     , running_sum_totalprecip_mm
     , ROUND(100 - ((diff_temp + diff_humidity + diff_totalprecip) / 3 * 100)) AS weather_comfort_score
from source_data