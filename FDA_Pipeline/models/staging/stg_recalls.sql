WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_fda_recalls') }}
),
cleaned AS (
    SELECT 
        -- identifiers
        recall_number,

        -- firm info
        LOWER(TRIM(REPLACE(recalling_firm, ',', ' '))) AS recalling_firm,
        city,state,country,

        -- recall info
        classification,
        status,
        voluntary_mandated,
        initial_firm_notification,
        distribution_pattern,
        reason_for_recall,
        product_description,
        product_quantity,
        code_info,

        -- convert date strings to proper dates
        TO_DATE(recall_initiation_date, 'YYYYMMDD') AS recall_initiation_date,
        TO_DATE(termination_date, 'YYYYMMDD') AS recall_termination_date,
        TO_DATE(report_date, 'YYYYMMDD') AS report_date,
        TO_DATE(center_classification_date, 'YYYYMMDD') AS center_classification_date,

        -- how long recall lasted (if terminated)
        CASE
            WHEN termination_date IS NOT NULL THEN TO_DATE(termination_date, 'YYYYMMDD') - TO_DATE(recall_initiation_date, 'YYYYMMDD')
            ELSE CURRENT_DATE - TO_DATE(recall_initiation_date, 'YYYYMMDD')
        END AS recall_duration_days,
        ingested_at
    From source
)

SELECT * FROM cleaned