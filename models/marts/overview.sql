-- models/marts/overview.sql
with source_data as (
    select * from {{ ref('source_data') }}
)
select
    date AS date,
    trafficSource.medium AS channel,
    trafficSource.source AS source,
    FORMAT_DATE("%w", adjusted_date) AS weekday,
    device.deviceCategory AS device_type,
    COUNT(*) AS total_sessions,
    COUNT(DISTINCT fullVisitorId) AS total_users,
    SUM(totals.pageviews) AS total_pageviews,
    SUM(IF(totals.bounces = 1, 1, 0)) / COUNT(*) AS bounce_rate,
    SUM(IF(totals.bounces = 1, 1, 0)) AS total_bounce,
    AVG(totals.timeOnSite / 60) AS average_session_duration_minutes
from
    source_data
group by
    date, weekday, source, device_type, channel
order by
    date DESC, weekday, source, device_type, channel
