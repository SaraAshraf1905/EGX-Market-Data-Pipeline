-- fails if any rows come back
select symbol, full_date, count(*) c
from {{ ref('stg_prices') }}
group by symbol, full_date having count(*) > 1
