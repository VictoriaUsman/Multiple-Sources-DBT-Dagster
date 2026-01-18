SELECT
    CITY,
    COUNT(*) AS TOTAL_OBSERVATIONS,            -- Added alias here
    LISTAGG(DISTINCT CONDITION, ', ') AS WEATHER_TYPES,
    LISTAGG(DISTINCT TEMP_C, ', ') AS TEMP_RANGE,
    CURRENT_TIMESTAMP() AS EXTRACTED_AT        -- Added () for standard function call
FROM {{ ref('bronze_weather') }}
GROUP BY CITY