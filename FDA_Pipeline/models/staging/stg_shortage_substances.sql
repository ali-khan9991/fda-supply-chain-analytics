WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_fda_shortages') }}
),
exploded AS (
    SELECT
        package_ndc,
        TRIM(json_array_elements_text("openfda.substance_name"::json)) AS substance_name
    FROM source
    WHERE "openfda.substance_name" IS NOT NULL
)
SELECT * FROM exploded