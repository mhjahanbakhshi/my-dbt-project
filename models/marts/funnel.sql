-- models/marts/funnel.sql
with my_action_types as (
    select 1 as action_type, 'Click through of product lists' as action_description union all
    select 2, 'Product detail views' union all
    select 5, 'Check out' union all
    select 6, 'Completed purchase'
),
funnel as (
    select
        adjusted_date AS date,
        hits.eCommerceAction.action_type as action,
        count(fullVisitorID) as users,
        mat.action_description as action_description
    from
        {{ ref('source_data') }} as bq,
        UNNEST(hits) as hits,
        UNNEST(hits.product) as product
    left join my_action_types as mat
        on hits.eCommerceAction.action_type = cast(mat.action_type as string)
    where
        hits.eCommerceAction.action_type in ('1', '2', '5', '6')
    group by
        date, action, action_description
    order by
        date asc, action asc
)
select
    date,
    max(case when action = '1' then users else 0 end) as Click_through_of_product_lists,
    max(case when action = '2' then users else 0 end) as Product_detail_views,
    max(case when action = '5' then users else 0 end) as Check_out,
    max(case when action = '6' then users else 0 end) as Completed_purchase,
    max(case when action = '1' then users else 0 end) / nullif(max(case when action = '1' then users else 0 end), 0) as PRECENT_Click_through_of_product_lists,
    max(case when action = '2' then users else 0 end) / nullif(max(case when action = '1' then users else 0 end), 0) as PRECENT_Product_detail_views,
    max(case when action = '5' then users else 0 end) / nullif(max(case when action = '1' then users else 0 end), 0) as PRECENT_Check_out,
    max(case when action = '6' then users else 0 end) / nullif(max(case when action = '1' then users else 0 end), 0) as PRECENT_Completed_purchase
from funnel
where
    date < current_date()
group by date
order by date asc
