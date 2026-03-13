WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_fda_shortages') }}
),

cleaned AS (
    SELECT
        -- identifiers
        package_ndc,

        -- drug info
        generic_name,
        ARRAY_TO_STRING(ARRAY(SELECT json_array_elements_text("openfda.brand_name"::json)), ', ') AS brand_name,
        ARRAY_TO_STRING(ARRAY(SELECT json_array_elements_text("openfda.route"::json)), ', ') AS route,

        -- shortage info
        status,

        -- fix availability typos
        CASE
            WHEN LOWER(availability) LIKE '%availablity%'  THEN 'Limited Availability'
            WHEN LOWER(availability) LIKE '%availabiltiy%' THEN 'Limited Availability'
            WHEN LOWER(availability) LIKE '%limited%'      THEN 'Limited Availability'
            ELSE availability
        END AS availability,

        shortage_reason,
        update_type,

        -- convert date strings to proper dates
        TO_DATE(initial_posting_date, 'MM/DD/YYYY') AS initial_posting_date,
        TO_DATE(update_date, 'MM/DD/YYYY')    AS last_updated_date,

        -- how long shortage has lasted
        CURRENT_DATE - TO_DATE(initial_posting_date, 'MM/DD/YYYY') AS shortage_days,

        ingested_at

    FROM source
)

SELECT * FROM cleaned