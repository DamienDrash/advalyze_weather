{{ config(materialized='view') }}

SELECT  city
     , PARSE_DATE('%Y-%m-%d',  date) AS date
     , avg_temp
     , totalprecip_mm
     , SUM(avg_temp) OVER(PARTITION BY city ORDER BY PARSE_DATE('%Y-%m-%d',  date)) AS running_sum_avg_temp
     , SUM(totalprecip_mm) OVER(PARTITION BY city ORDER BY PARSE_DATE('%Y-%m-%d',  date)) AS running_sum_totalprecip_mm
FROM `advalyze-355820.weather.weather_l7days`