-- METRIC 6: every symbol rebased to 100 on the SAME start date (fair comparison, e.g. stock vs EGX30)
with base as (
    select symbol, min(full_date) as base_date
    from {{ ref('stg_prices') }}
    where full_date >= '{{ var("compare_start_date") }}'
    group by symbol
),
common as (select max(base_date) as start_date from base)   -- latest first-date = date all symbols exist
select p.symbol, p.full_date,
       100.0 * p.adj_close_price / nullif(b.adj_close_price, 0) as rebased_100
from {{ ref('stg_prices') }} p
cross join common c
join {{ ref('stg_prices') }} b on b.symbol = p.symbol and b.full_date = (
        select min(x.full_date) from {{ ref('stg_prices') }} x where x.symbol = p.symbol and x.full_date >= c.start_date)
where p.full_date >= c.start_date
