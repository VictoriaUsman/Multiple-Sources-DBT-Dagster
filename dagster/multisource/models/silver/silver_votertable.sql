SELECT
    TRIM(UPPER(CITY)) AS CITY_NAME, -- Clean up names for better grouping
    COUNT(*) AS VOTE_AS_OF_2O26 ,
    CURRENT_TIMESTAMP() AS CALCULATION_TIME
FROM {{ ref('bronze_votertable') }}
GROUP BY city-- This refers to the first column (CITY_NAME)a