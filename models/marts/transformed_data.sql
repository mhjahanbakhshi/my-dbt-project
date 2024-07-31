-- models/marts/transformed_data.sql

with transformed_data as (
    select * from {{ ref('source_data') }}
)
select * from transformed_data
