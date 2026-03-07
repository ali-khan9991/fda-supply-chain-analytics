WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_fda_recalls') }}
),

exploded AS (
    SELECT
        recall_number,
        TRIM(json_array_elements_text("openfda.manufacturer_name"::json)) AS manufacturer_name
    FROM source
    WHERE "openfda.manufacturer_name" IS NOT NULL
)

SELECT * FROM exploded