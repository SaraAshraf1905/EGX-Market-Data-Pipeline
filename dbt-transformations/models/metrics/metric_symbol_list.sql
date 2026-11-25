-- Slicer table for Power BI: one row per symbol that has prices
select distinct s.symbol, s.company_name, s.asset_class
from {{ source('gold', 'dim_symbol') }} s
where exists (select 1 from {{ ref('stg_prices') }} p where p.symbol = s.symbol)
