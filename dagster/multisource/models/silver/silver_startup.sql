{{ config(schema='silver') }}

SELECT
    {{ trim_all_columns(ref('bronze_startup'), exclude_list=['CHANGE_IN_POSITION']) }},
    CURRENT_TIMESTAMP() AS PROCESSED_AT
FROM {{ ref('bronze_startup') }}