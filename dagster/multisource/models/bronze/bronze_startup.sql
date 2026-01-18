

SELECT*
FROM {{ source('snowflake_staging', 'STARTUP_CITIES') }}