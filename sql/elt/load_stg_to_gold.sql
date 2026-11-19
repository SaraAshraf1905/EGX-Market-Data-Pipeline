USE [EGX_Warehouse];
GO

SET NOCOUNT ON;
BEGIN TRY
    BEGIN TRANSACTION;

    -- 1. تسجيل بداية الـ Run في جدول المراقبة
    DECLARE @run_id BIGINT;
    INSERT INTO ops.pipeline_runs (step_name, status, started_at)
    VALUES ('stg_to_gold_elt', 'running', SYSDATETIME());
    SET @run_id = SCOPE_IDENTITY();

    -- 2. تعبئة dim_exchange بدون أي NULL
    MERGE gold.dim_exchange AS t
    USING (SELECT 'EGX' AS exchange_code) AS s
    ON t.exchange_code = s.exchange_code
    WHEN MATCHED THEN UPDATE SET
        t.exchange_name = 'Egyptian Exchange',
        t.country_code  = 'EG',
        t.currency_code = 'EGP',
        t.timezone      = 'Africa/Cairo',
        t.weekend_days  = 'Friday,Saturday',
        t.yahoo_suffix  = '.CA'
    WHEN NOT MATCHED THEN
        INSERT (exchange_code, exchange_name, country_code, currency_code, timezone, weekend_days, yahoo_suffix)
        VALUES ('EGX', 'Egyptian Exchange', 'EG', 'EGP', 'Africa/Cairo', 'Friday,Saturday', '.CA');

    DECLARE @egx_key INT = (SELECT exchange_key FROM gold.dim_exchange WHERE exchange_code = 'EGX');

    -- 3. تعبئة dim_source (licence_note فقط يُترك NULL)
    MERGE gold.dim_source AS t
    USING (VALUES
        ('yahoo_egx_daily',  'Yahoo Finance EGX Daily', 'API / Historical', 'Yahoo Finance', 'Silver CSV', 1),
        ('silver_dividends', 'EGX Dividends Summary',   'CSV',              'EGX / ICE',     'Silver CSV', 1),
        ('egx30_historical', 'EGX30 Daily Index',       'CSV',              'EGX',           'Silver CSV', 1)
    ) AS s (source_code, source_name, source_type, provider, landing_path, is_active)
    ON t.source_code = s.source_code
    WHEN MATCHED THEN UPDATE SET
        t.source_name  = s.source_name,
        t.source_type  = s.source_type,
        t.provider     = s.provider,
        t.landing_path = s.landing_path,
        t.is_active    = s.is_active
    WHEN NOT MATCHED THEN
        INSERT (source_code, source_name, source_type, provider, landing_path, is_active)
        VALUES (s.source_code, s.source_name, s.source_type, s.provider, s.landing_path, s.is_active);

    -- 4. تجميع كل التواريخ وتعبئة dim_date + تحديث الأجازات والويك إند
    WITH AllDates AS (
        SELECT DISTINCT TRY_CAST(trade_date AS DATE) AS dt FROM stg.stock_prices WHERE trade_date IS NOT NULL
        UNION
        SELECT DISTINCT TRY_CAST(trade_date AS DATE) FROM stg.egx30_daily WHERE trade_date IS NOT NULL
        UNION
        SELECT DISTINCT TRY_CAST(snapshot_date AS DATE) FROM stg.dividends_snapshot WHERE snapshot_date IS NOT NULL
        UNION
        SELECT DISTINCT DATEFROMPARTS(column1, column2, column3) 
        FROM stg.holidays 
        WHERE column1 IS NOT NULL AND column2 IS NOT NULL AND column3 IS NOT NULL
    )
    INSERT INTO gold.dim_date (
        date_key, full_date, [year], [quarter], [month], month_name, year_month,
        week_of_year, day_of_month, day_of_week, day_name,
        is_weekend, is_month_end, is_market_holiday, is_egx_trading_day
    )
    SELECT
        CAST(FORMAT(dt, 'yyyyMMdd') AS INT),
        dt, YEAR(dt), DATEPART(QUARTER, dt), MONTH(dt), DATENAME(MONTH, dt), FORMAT(dt, 'yyyy-MM'),
        DATEPART(WEEK, dt), DAY(dt), DATEPART(WEEKDAY, dt), DATENAME(WEEKDAY, dt),
        CASE WHEN DATENAME(WEEKDAY, dt) IN ('Friday', 'Saturday') THEN 1 ELSE 0 END,
        CASE WHEN dt = EOMONTH(dt) THEN 1 ELSE 0 END,
        0,
        CASE WHEN DATENAME(WEEKDAY, dt) IN ('Friday', 'Saturday') THEN 0 ELSE 1 END
    FROM AllDates
    WHERE dt IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM gold.dim_date d WHERE d.full_date = AllDates.dt);

    -- تحديث is_market_holiday و is_egx_trading_day بدون أي NULL
    UPDATE d
    SET d.is_market_holiday  = CASE WHEN h.column1 IS NOT NULL THEN 1 ELSE 0 END,
        d.is_egx_trading_day = CASE WHEN d.is_weekend = 1 OR h.column1 IS NOT NULL THEN 0 ELSE 1 END
    FROM gold.dim_date d
    LEFT JOIN stg.holidays h 
        ON d.full_date = DATEFROMPARTS(h.column1, h.column2, h.column3);

    -- 5. دمج بيانات الأسهم في dim_symbol (تعبئة yahoo_ticker و asset_class وبيانات الشركات)
    MERGE gold.dim_symbol AS t
    USING (
        SELECT
            UPPER(LTRIM(RTRIM(COALESCE(symbol_clean, REPLACE(symbol, '.CA', ''))))) AS clean_sym,
            MAX([description]) AS company_name,
            MAX(isin) AS isin,
            MAX(COALESCE([type], 'stock')) AS instrument_type,
            MAX(typespecs) AS share_class,
            MAX(COALESCE(currency, CASE WHEN is_usd = 'True' OR is_usd = '1' THEN 'USD' ELSE 'EGP' END)) AS currency_code,
            MAX(CASE WHEN is_primary = 'True' OR is_primary = '1' THEN 1 ELSE 0 END) AS is_primary_listing,
            MAX(logoid) AS logo_id,
            MAX(provider) AS metadata_provider,
            MAX(CASE WHEN found_by_isin = 'True' OR found_by_isin = '1' THEN 1 ELSE 0 END) AS found_by_isin
        FROM stg.symbols
        WHERE COALESCE(symbol_clean, symbol) IS NOT NULL
        GROUP BY UPPER(LTRIM(RTRIM(COALESCE(symbol_clean, REPLACE(symbol, '.CA', '')))))
    ) AS s
    ON t.symbol = s.clean_sym
    WHEN MATCHED THEN UPDATE SET
        t.yahoo_ticker       = s.clean_sym + '.CA',
        t.isin               = COALESCE(s.isin, t.isin),
        t.company_name       = COALESCE(s.company_name, t.company_name),
        t.asset_class        = 'Equities',
        t.instrument_type    = COALESCE(s.instrument_type, t.instrument_type),
        t.share_class        = COALESCE(s.share_class, t.share_class),
        t.currency_code      = s.currency_code,
        t.is_primary_listing = s.is_primary_listing,
        t.logo_id            = COALESCE(s.logo_id, t.logo_id),
        t.metadata_provider  = COALESCE(s.metadata_provider, t.metadata_provider),
        t.found_by_isin      = s.found_by_isin,
        t.has_metadata       = 1,
        t.updated_at         = SYSDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (symbol, yahoo_ticker, isin, company_name, asset_class, instrument_type, share_class,
                currency_code, is_primary_listing, exchange_key, logo_id, metadata_provider,
                found_by_isin, has_metadata, has_price_data)
        VALUES (s.clean_sym, s.clean_sym + '.CA', s.isin, s.company_name, 'Equities', s.instrument_type,
                s.share_class, s.currency_code, s.is_primary_listing, @egx_key, s.logo_id,
                s.metadata_provider, s.found_by_isin, 1, 0);

    -- إضافة أي أسهم ظهرت في الأسعار أو التوزيعات + مؤشر EGX30 لو مش موجودين
    WITH ExtraSymbols AS (
        SELECT DISTINCT UPPER(LTRIM(RTRIM(COALESCE(symbol_clean, REPLACE(symbol, '.CA', ''))))) AS sym,
               'Equities' AS asset_cls, 'stock' AS inst_type
        FROM stg.stock_prices
        UNION
        SELECT DISTINCT UPPER(LTRIM(RTRIM(COALESCE(symbol_clean, symbol)))), 'Equities', 'stock'
        FROM stg.dividends_snapshot
        UNION
        SELECT DISTINCT UPPER(LTRIM(RTRIM(index_code))), 'Indices', 'Index'
        FROM stg.egx30_daily
    )
    INSERT INTO gold.dim_symbol (
        symbol, yahoo_ticker, asset_class, instrument_type, currency_code,
        is_primary_listing, exchange_key, found_by_isin, has_metadata, has_price_data
    )
    SELECT
        e.sym,
        CASE WHEN e.inst_type = 'Index' THEN NULL ELSE e.sym + '.CA' END,
        e.asset_cls, e.inst_type, 'EGP', 1, @egx_key, 0, 0, 1
    FROM ExtraSymbols e
    WHERE e.sym IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM gold.dim_symbol d WHERE d.symbol = e.sym);

    -- 6. تحميل الأسعار اليومية في fact_daily_price
    MERGE gold.fact_daily_price AS t
    USING (
        SELECT
            sym.symbol_key,
            CAST(FORMAT(TRY_CAST(p.trade_date AS DATE), 'yyyyMMdd') AS INT) AS date_key,
            src.source_key,
            TRY_CAST(p.[open] AS DECIMAL(18,4)) AS open_price,
            TRY_CAST(p.high AS DECIMAL(18,4)) AS high_price,
            TRY_CAST(p.low AS DECIMAL(18,4)) AS low_price,
            TRY_CAST(p.[close] AS DECIMAL(18,4)) AS close_price,
            TRY_CAST(p.adj_close AS DECIMAL(18,4)) AS adj_close_price,
            COALESCE(TRY_CAST(p.volume AS BIGINT), 0) AS volume,
            CAST(TRY_CAST(p.[close] AS DECIMAL(18,4)) * COALESCE(TRY_CAST(p.volume AS BIGINT), 0) AS DECIMAL(22,3)) AS trade_value,
            COALESCE(TRY_CAST(p.dividends AS DECIMAL(18,6)), 0) AS dividend_amount,
            COALESCE(TRY_CAST(p.stock_splits AS DECIMAL(12,6)), TRY_CAST(p.splits AS DECIMAL(12,6)), 0) AS stock_split_ratio,
            COALESCE(TRY_CAST(p.capital_gains AS DECIMAL(18,6)), 0) AS capital_gains,
            CASE WHEN p.is_traded = 'True' OR p.is_traded = '1' THEN 1 ELSE 0 END AS is_traded,
            TRY_CAST(REPLACE(p.fetched_at_utc, 'Z', '') AS DATETIME2(0)) AS fetched_at_utc,
            RIGHT(p.source_file, CHARINDEX('/', REVERSE(p.source_file) + '/') - 1) AS clean_file
        FROM stg.stock_prices p
        INNER JOIN gold.dim_symbol sym
            ON sym.symbol = UPPER(LTRIM(RTRIM(COALESCE(p.symbol_clean, REPLACE(p.symbol, '.CA', '')))))
        INNER JOIN gold.dim_source src
            ON src.source_code = COALESCE(p.source, 'yahoo_egx_daily')
        WHERE p.[close] IS NOT NULL AND p.trade_date IS NOT NULL
    ) AS s
    ON t.symbol_key = s.symbol_key AND t.date_key = s.date_key AND t.source_key = s.source_key
    WHEN MATCHED THEN UPDATE SET
        t.open_price = s.open_price, t.high_price = s.high_price, t.low_price = s.low_price,
        t.close_price = s.close_price, t.adj_close_price = s.adj_close_price, t.volume = s.volume,
        t.trade_value = s.trade_value, t.dividend_amount = s.dividend_amount,
        t.stock_split_ratio = s.stock_split_ratio, t.capital_gains = s.capital_gains,
        t.is_traded = s.is_traded, t.dq_flags = 'OK', t.source_file = s.clean_file,
        t.run_id = @run_id, t.loaded_at = SYSDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (symbol_key, date_key, source_key, open_price, high_price, low_price, close_price,
                adj_close_price, volume, trade_value, dividend_amount, stock_split_ratio,
                capital_gains, is_traded, dq_flags, fetched_at_utc, source_file, mongo_doc_id, run_id)
        VALUES (s.symbol_key, s.date_key, s.source_key, s.open_price, s.high_price, s.low_price,
                s.close_price, s.adj_close_price, s.volume, s.trade_value, s.dividend_amount,
                s.stock_split_ratio, s.capital_gains, s.is_traded, 'OK', s.fetched_at_utc,
                s.clean_file, NULL, @run_id);

    -- 7. تحميل بيانات مؤشر EGX30 في fact_daily_price
    MERGE gold.fact_daily_price AS t
    USING (
        SELECT
            sym.symbol_key,
            CAST(FORMAT(TRY_CAST(e.trade_date AS DATE), 'yyyyMMdd') AS INT) AS date_key,
            src.source_key,
            TRY_CAST(e.[open] AS DECIMAL(18,4)) AS open_price,
            TRY_CAST(e.high AS DECIMAL(18,4)) AS high_price,
            TRY_CAST(e.low AS DECIMAL(18,4)) AS low_price,
            TRY_CAST(e.[close] AS DECIMAL(18,4)) AS close_price,
            TRY_CAST(e.[close] AS DECIMAL(18,4)) AS adj_close_price,
            COALESCE(TRY_CAST(e.volume AS BIGINT), 0) AS volume,
            CASE WHEN e.is_traded = 'True' OR e.is_traded = '1' THEN 1 ELSE 0 END AS is_traded,
            TRY_CAST(REPLACE(e.datetime_utc, 'Z', '') AS DATETIME2(0)) AS fetched_at_utc,
            RIGHT(e.source_file, CHARINDEX('/', REVERSE(e.source_file) + '/') - 1) AS clean_file
        FROM stg.egx30_daily e
        INNER JOIN gold.dim_symbol sym ON sym.symbol = UPPER(LTRIM(RTRIM(e.index_code)))
        INNER JOIN gold.dim_source src ON src.source_code = 'egx30_historical'
        WHERE e.[close] IS NOT NULL AND e.trade_date IS NOT NULL
    ) AS s
    ON t.symbol_key = s.symbol_key AND t.date_key = s.date_key AND t.source_key = s.source_key
    WHEN NOT MATCHED THEN
        INSERT (symbol_key, date_key, source_key, open_price, high_price, low_price, close_price,
                adj_close_price, volume, trade_value, dividend_amount, stock_split_ratio,
                capital_gains, is_traded, dq_flags, fetched_at_utc, source_file, mongo_doc_id, run_id)
        VALUES (s.symbol_key, s.date_key, s.source_key, s.open_price, s.high_price, s.low_price,
                s.close_price, s.adj_close_price, s.volume, 0, 0, 0, 0, s.is_traded, 'OK',
                s.fetched_at_utc, s.clean_file, NULL, @run_id);

    -- 8. تحميل التوزيعات في fact_dividend_snapshot
    DECLARE @div_src_key INT = (SELECT source_key FROM gold.dim_source WHERE source_code = 'silver_dividends');

    MERGE gold.fact_dividend_snapshot AS t
    USING (
        SELECT
            sym.symbol_key,
            TRY_CAST(d.snapshot_date AS DATE) AS snapshot_date,
            @div_src_key AS source_key,
            d.dividendtype AS dividend_type,
            TRY_CAST(d.amountrecent AS DECIMAL(18,6)) AS amount_recent,
            TRY_CAST(CAST(d.exdaterecent_date AS VARCHAR(20)) AS DATE) AS ex_date_recent,
            TRY_CAST(d.amountupcoming AS DECIMAL(18,6)) AS amount_upcoming,
            TRY_CAST(d.exdateupcoming_date AS DATE) AS ex_date_upcoming,
            COALESCE(TRY_CAST(d.yield AS DECIMAL(9,4)), 0.0000) AS yield_pct
        FROM stg.dividends_snapshot d
        INNER JOIN gold.dim_symbol sym
            ON sym.symbol = UPPER(LTRIM(RTRIM(COALESCE(d.symbol_clean, d.symbol))))
        WHERE d.snapshot_date IS NOT NULL
    ) AS s
    ON t.symbol_key = s.symbol_key AND t.snapshot_date = s.snapshot_date
    WHEN MATCHED THEN UPDATE SET
        t.dividend_type = s.dividend_type, t.amount_recent = s.amount_recent,
        t.ex_date_recent = s.ex_date_recent, t.amount_upcoming = s.amount_upcoming,
        t.ex_date_upcoming = s.ex_date_upcoming, t.yield_pct = s.yield_pct,
        t.run_id = @run_id
    WHEN NOT MATCHED THEN
        INSERT (symbol_key, snapshot_date, source_key, dividend_type, amount_recent,
                ex_date_recent, amount_upcoming, ex_date_upcoming, yield_pct, run_id)
        VALUES (s.symbol_key, s.snapshot_date, s.source_key, s.dividend_type, s.amount_recent,
                s.ex_date_recent, s.amount_upcoming, s.ex_date_upcoming, s.yield_pct, @run_id);

    -- 9. تحديث علامة has_price_data وإغلاق الـ Run بنجاح
    UPDATE sym
    SET sym.has_price_data = 1, sym.updated_at = SYSDATETIME()
    FROM gold.dim_symbol sym
    WHERE EXISTS (SELECT 1 FROM gold.fact_daily_price f WHERE f.symbol_key = sym.symbol_key);

    DECLARE @rows_cnt BIGINT = (SELECT COUNT(*) FROM gold.fact_daily_price WHERE run_id = @run_id);
    UPDATE ops.pipeline_runs
    SET status = 'success',
        rows_written = @rows_cnt,
        message = 'ELT completed successfully',
        finished_at = SYSDATETIME()
    WHERE run_id = @run_id;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    UPDATE ops.pipeline_runs
    SET status = 'failed',
        message = LEFT(ERROR_MESSAGE(), 500),
        finished_at = SYSDATETIME()
    WHERE run_id = @run_id;
    THROW;
END CATCH;
GO