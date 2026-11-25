-- a >100% one-day move on a stock is almost always bad data (split not adjusted, wrong price)
select * from {{ ref('metric_daily_return') }} where abs(daily_return) > 1
