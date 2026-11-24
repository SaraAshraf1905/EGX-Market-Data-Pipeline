-- METRIC 2: price rebased to 100 at the symbol's first date (compare assets on one scale)
select
    symbol, asset_class, full_date,
    100.0 * adj_close_price
      / first_value(adj_close_price) over (partition by symbol order by full_date
                                           rows between unbounded preceding and unbounded following)
        as rebased_100
from {{ ref('stg_prices') }}
