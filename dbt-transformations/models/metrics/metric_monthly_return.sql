-- METRIC 5: monthly return = last close of month / last close of previous month - 1
with m as (
    select symbol, year, month, full_date, adj_close_price,
           row_number() over (partition by symbol, year, month order by full_date desc) as rn
    from {{ ref('stg_prices') }}
),
me as (select symbol, year, month, adj_close_price as month_end_price from m where rn = 1)
select symbol, year, month, month_end_price,
       month_end_price / nullif(lag(month_end_price) over (partition by symbol order by year, month), 0) - 1
         as monthly_return
from me
