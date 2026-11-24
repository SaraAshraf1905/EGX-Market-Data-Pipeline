-- METRIC 1: daily return = today / previous traded day - 1 (uses adjusted close)
select
    symbol, asset_class, full_date, adj_close_price,
    adj_close_price / nullif(lag(adj_close_price) over (partition by symbol order by full_date), 0) - 1
        as daily_return
from {{ ref('stg_prices') }}
