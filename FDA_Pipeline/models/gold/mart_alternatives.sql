WITH shortage_substances AS (
    -- get all substances currently in shortage
    SELECT DISTINCT
        s.package_ndc,
        s.generic_name,
        s.status,
        s.shortage_days,
        ss.substance_name
    FROM {{ ref('stg_shortages') }} s
    JOIN {{ ref('stg_shortage_substances') }} ss
        ON s.package_ndc = ss.package_ndc
    WHERE s.status = 'Current'
),

approved_manufacturers AS (
    -- get all approved manufacturers per substance
    SELECT DISTINCT
        aps.substance_name,
        apm.manufacturer_name
    FROM {{ ref('stg_approval_substances') }} aps
    JOIN {{ ref('stg_approval_manufacturers') }} apm
        ON aps.application_number = apm.application_number
),

shortage_manufacturers AS (
    -- get all manufacturers currently in shortage
    SELECT DISTINCT
        LOWER(TRIM(REPLACE(manufacturer_name, ',', ''))) AS manufacturer_name_clean
    FROM {{ ref('stg_shortage_manufacturers') }}
),

alternatives AS (
    SELECT
        sh.package_ndc,
        sh.generic_name,
        sh.substance_name,
        sh.shortage_days,
        am.manufacturer_name AS alternative_manufacturer,
        LOWER(TRIM(REPLACE(am.manufacturer_name, ',', ''))) AS manufacturer_name_clean
    FROM shortage_substances sh
    JOIN approved_manufacturers am
        ON LOWER(TRIM(sh.substance_name)) = LOWER(TRIM(am.substance_name))
)

SELECT
    package_ndc,
    generic_name,
    substance_name,
    shortage_days,
    alternative_manufacturer
FROM alternatives
WHERE manufacturer_name_clean NOT IN (
    SELECT manufacturer_name_clean FROM shortage_manufacturers
)
ORDER BY shortage_days DESC