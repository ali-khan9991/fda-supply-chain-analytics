WITH shortage_base AS (
    SELECT
        s.package_ndc,
        s.generic_name,
        s.brand_name,
        s.route,
        s.status,
        s.availability,
        s.shortage_reason,
        s.shortage_days,
        s.initial_posting_date,
        s.last_updated_date
    FROM {{ ref('stg_shortages') }} s
    WHERE s.status = 'Current'
),
manufacturer_risk AS (
    SELECT
        sm.package_ndc,
        MAX(mr.risk_score)  AS max_manufacturer_risk_score,
        MAX(mr.risk_level)  AS worst_risk_level
    FROM {{ ref('stg_shortage_manufacturers') }} sm
    LEFT JOIN {{ ref('mart_manufacturer_risk') }} mr
        ON LOWER(TRIM(REPLACE(sm.manufacturer_name, ',', '')))
         = LOWER(TRIM(REPLACE(mr.manufacturer_name, ',', '')))
    GROUP BY sm.package_ndc
),
alternatives_count AS (
    SELECT
        package_ndc,
        COUNT(DISTINCT alternative_manufacturer) AS alternatives_available
    FROM {{ ref('mart_alternatives') }}
    GROUP BY package_ndc
),
combined AS (
    SELECT
        sb.package_ndc,
        sb.generic_name,
        sb.brand_name,
        sb.route,
        sb.status,
        sb.availability,
        sb.shortage_reason,
        sb.shortage_days,
        sb.initial_posting_date,
        sb.last_updated_date,
        COALESCE(mr.max_manufacturer_risk_score, 0) AS manufacturer_risk_score,
        COALESCE(mr.worst_risk_level, 'Low')        AS manufacturer_risk_level,
        COALESCE(ac.alternatives_available, 0)      AS alternatives_available,
        CASE
            WHEN sb.shortage_days > 365  THEN 3 ELSE 1
        END +
        CASE
            WHEN mr.worst_risk_level = 'High'   THEN 3
            WHEN mr.worst_risk_level = 'Medium' THEN 2
            ELSE 1
        END +
        CASE
            WHEN COALESCE(ac.alternatives_available, 0) = 0  THEN 3
            WHEN COALESCE(ac.alternatives_available, 0) < 3  THEN 2
            ELSE 1
        END AS shortage_risk_score
    FROM shortage_base sb
    LEFT JOIN manufacturer_risk mr ON sb.package_ndc = mr.package_ndc
    LEFT JOIN alternatives_count ac ON sb.package_ndc = ac.package_ndc
)
SELECT
    package_ndc,
    generic_name,
    brand_name,
    route,
    status,
    availability,
    shortage_reason,
    shortage_days,
    initial_posting_date,
    last_updated_date,
    manufacturer_risk_score,
    manufacturer_risk_level,
    alternatives_available,
    shortage_risk_score,
    CASE
        WHEN shortage_risk_score >= 7 THEN 'Critical'
        WHEN shortage_risk_score >= 5 THEN 'High'
        WHEN shortage_risk_score >= 3 THEN 'Medium'
        ELSE 'Low'
    END AS shortage_risk_level
FROM combined
ORDER BY shortage_risk_score DESC