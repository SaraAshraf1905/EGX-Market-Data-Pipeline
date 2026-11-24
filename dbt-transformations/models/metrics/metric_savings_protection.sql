{{ config(enabled = var('enable_gold_fx')) }}
-- Only built when enable_gold_fx=true (gold and USD/EGP must exist in gold.fact_daily_price).
-- Gold in EGP = gold (USD) x USD/EGP. Rebased to 100 from compare_start_date.
with gold as (
    select g.full_date, g.adj_close_price * fx.adj_close_price as price_egp
    from {{ ref('stg_prices') }} g
    join {{ ref('stg_prices') }} fx on fx.full_date = g.full_date and fx.symbol = '{{ var("fx_symbol") }}'
    where g.symbol = '{{ var("gold_symbol") }}' and g.full_date >= '{{ var("compare_start_date") }}'
),
asset as (
    select symbol, full_date, adj_close_price from {{ ref('stg_prices') }}
    where full_date >= '{{ var("compare_start_date") }}'
    union all
    select 'GOLD_EGP', full_date, price_egp from gold
)
select symbol, full_date,
       100.0 * adj_close_price / nullif(first_value(adj_close_price) over (partition by symbol order by full_date), 0) as rebased_100
from asset
