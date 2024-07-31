-- models/staging/source_data.sql
with source_data as (
    SELECT
    *,
    DATE_ADD(PARSE_DATE("%Y%m%d", date), INTERVAL 2597 DAY) AS adjusted_date
FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
    DATE_ADD(PARSE_DATE("%Y%m%d", date), INTERVAL 2597 DAY) < CURRENT_DATE()
)
select * from source_data
