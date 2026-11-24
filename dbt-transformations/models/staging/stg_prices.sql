-- One clean row per traded symbol/day. Only the needed columns are sorted (keeps memory small).
with f as (
    select symbol_key, date_key, source_key, close_price, adj_close_price, volume,
           row_number() over (partition by symbol_key, date_key order by source_key) as rn
    from {{ source('gold', 'fact_daily_price') }}
    where is_traded = 1 and adj_close_price is not null
)
select s.symbol, s.yahoo_ticker, s.asset_class, d.full_date, d.year, d.month,
       f.symbol_key, f.date_key, f.close_price, f.adj_close_price, f.volume
from f
join {{ source('gold', 'dim_symbol') }} s on s.symbol_key = f.symbol_key
join {{ source('gold', 'dim_date') }}   d on d.date_key   = f.date_key
where f.rn = 1
