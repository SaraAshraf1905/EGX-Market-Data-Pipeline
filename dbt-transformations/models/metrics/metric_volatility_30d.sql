-- METRIC 3: rolling volatility = stdev of daily returns over the last N rows, annualized
with r as (
    select symbol, full_date, daily_return
    from {{ ref('metric_daily_return') }}
    where daily_return is not null
),
w as (
    select symbol, full_date,
           stdev(daily_return) over (partition by symbol order by full_date
                                     rows between {{ var('vol_window_days') - 1 }} preceding and current row) as sd,
           count(daily_return) over (partition by symbol order by full_date
                                     rows between {{ var('vol_window_days') - 1 }} preceding and current row) as n
    from r
)
select symbol, full_date,
       sd * sqrt({{ var('trading_days_per_year') }}) as volatility_annualized
from w
where n >= 20   -- need at least 20 observations
