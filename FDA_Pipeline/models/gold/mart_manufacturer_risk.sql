WITH shortage_manufacturers AS (
    SELECT
        sm.manufacturer_name,
        COUNT(DISTINCT sm.package_ndc) AS active_shortage_count
    FROM {{ ref('stg_shortage_manufacturers') }} sm
    JOIN {{ ref('stg_shortages') }} s
        ON sm.package_ndc = s.package_ndc
    WHERE s.status = 'Current'
    GROUP BY sm.manufacturer_name
),

recall_counts AS (
    SELECT
        recalling_firm AS manufacturer_name,
        COUNT(DISTINCT recall_number)                                     AS total_recalls,
        COUNT(DISTINCT CASE WHEN classification = 'Class I'
              THEN recall_number END)                                     AS class1_recalls,
        COUNT(DISTINCT CASE WHEN classification = 'Class II'
              THEN recall_number END)                                     AS class2_recalls,
        COUNT(DISTINCT CASE WHEN classification = 'Class III'
              THEN recall_number END)                                     AS class3_recalls,
        COUNT(DISTINCT CASE WHEN status = 'Ongoing'
              THEN recall_number END)                                     AS ongoing_recalls
    FROM {{ ref('stg_recalls') }}
    GROUP BY recalling_firm
),

combined AS (
    SELECT
        COALESCE(sm.manufacturer_name, rc.manufacturer_name) AS manufacturer_name,
        COALESCE(sm.active_shortage_count, 0)                AS active_shortage_count,
        COALESCE(rc.total_recalls, 0)                        AS total_recalls,
        COALESCE(rc.class1_recalls, 0)                       AS class1_recalls,
        COALESCE(rc.class2_recalls, 0)                       AS class2_recalls,
        COALESCE(rc.class3_recalls, 0)                       AS class3_recalls,
        COALESCE(rc.ongoing_recalls, 0)                      AS ongoing_recalls,

        (COALESCE(sm.active_shortage_count, 0) * 3) +
        (COALESCE(rc.class1_recalls, 0) * 5) +
        (COALESCE(rc.class2_recalls, 0) * 2) +
        (COALESCE(rc.class3_recalls, 0) * 1) +
        (COALESCE(rc.ongoing_recalls, 0) * 3)                AS risk_score

    FROM shortage_manufacturers sm
    FULL OUTER JOIN recall_counts rc
        ON LOWER(TRIM(REPLACE(sm.manufacturer_name, ',', '')))
         = LOWER(TRIM(REPLACE(rc.manufacturer_name, ',', '')))
)

SELECT
    manufacturer_name,
    active_shortage_count,
    total_recalls,
    class1_recalls,
    class2_recalls,
    class3_recalls,
    ongoing_recalls,
    risk_score,
    CASE
        WHEN risk_score >= 20 THEN 'High'
        WHEN risk_score >= 10 THEN 'Medium'
        ELSE 'Low'
    END AS risk_level
FROM combined
ORDER BY risk_score DESC