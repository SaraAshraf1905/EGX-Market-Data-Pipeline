select * from {{ ref('metric_drawdown') }} where drawdown > 0.0000001
