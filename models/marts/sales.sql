-- models/marts/sales.sql

with source_data as (
    select * from {{ ref('source_data') }}
),
unnested_data as (
    select 
        DATE_ADD(PARSE_DATE("%Y%m%d", date), INTERVAL 2436 DAY) AS date,
        hits,
        totals,
        product
    from 
        source_data,
        UNNEST(hits) as hits,
        UNNEST(hits.product) as product
)
select
    date,
    product.productSKU,
    product.v2ProductName,
    product.v2ProductCategory,
    COUNT(totals.transactions) as transactions,
    SUM(CASE WHEN totals.transactionRevenue > 0 THEN totals.transactionRevenue ELSE 0 END) / 1000000 as transactionRevenue
from
    unnested_data
group by
    date, product.productSKU, product.v2ProductName, product.v2ProductCategory
having
    transactions is not null
order by
    date
