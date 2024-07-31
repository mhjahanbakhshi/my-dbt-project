-- models/marts/top_pages.sql

with source_data as (
    select * from {{ ref('source_data') }}
),
unnested_data as (
    select
        adjusted_date AS date,
        FORMAT_DATE("%w", adjusted_date) AS weekday,
        page.pagePath AS pagePath,
        page.pageTitle AS pageTitle,
        page.pagePathLevel1 AS pagePathLevel1,
        page.pagePathLevel2 AS pagePathLevel2,
        page.pagePathLevel3 AS pagePathLevel3,
        page.pagePathLevel4 AS pagePathLevel4,
        totals.pageviews AS pageviews,
        totals.bounces AS bounces,
        totals.timeOnSite AS timeOnSite,
        fullVisitorId AS fullVisitorId
    from 
        source_data,
        UNNEST(hits) as hits
)
select
    date,
    weekday,
    pagePath,
    pageTitle,
    pagePathLevel1,
    pagePathLevel2,
    pagePathLevel3,
    pagePathLevel4,
    COUNT(*) AS total_sessions,
    COUNT(DISTINCT fullVisitorId) AS total_users,
    SUM(pageviews) AS total_pageviews,
    SUM(IF(bounces = 1, 1, 0)) / COUNT(*) AS bounce_rate,
    SUM(IF(bounces = 1, 1, 0)) AS total_bounce,
    AVG(timeOnSite / 60) AS average_session_duration_minutes
from
    unnested_data
where
    pagePath IS NOT NULL
group by
    date, weekday, pagePath, pageTitle, pagePathLevel1, pagePathLevel2, pagePathLevel3, pagePathLevel4
order by
    date
