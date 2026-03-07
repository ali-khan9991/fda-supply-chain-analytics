WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_fda_approvals') }}
),

cleaned AS (
    SELECT
        -- identifiers
        application_number,
        sponsor_name,

        -- drug info
        ARRAY_TO_STRING(
            ARRAY(SELECT json_array_elements_text("openfda.generic_name"::json)),
            ', '
        ) AS generic_names,

        ARRAY_TO_STRING(
            ARRAY(SELECT json_array_elements_text("openfda.brand_name"::json)),
            ', '
        ) AS brand_names,

        ARRAY_TO_STRING(
            ARRAY(SELECT json_array_elements_text("openfda.route"::json)),
            ', '
        ) AS routes,

        ARRAY_TO_STRING(
            ARRAY(SELECT json_array_elements_text("openfda.product_type"::json)),
            ', '
        ) AS product_types,

        ingested_at

    FROM source
    WHERE "openfda.generic_name" IS NOT NULL
)

SELECT * FROM cleaned