-- METRIC 4: drawdown = how far below the previous peak (0 = at peak, -0.25 = 25% below)
with p as (
    select symbol, full_date, adj_close_price,
           max(adj_close_price) over (partition by symbol order by full_date
                                      rows between unbounded preceding and current row) as running_peak
    from {{ ref('stg_prices') }}
)
select symbol, full_date, adj_close_price, running_peak,
       adj_close_price / nullif(running_peak, 0) - 1 as drawdown
from p
