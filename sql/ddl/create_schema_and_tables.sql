IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'EGX_Warehouse')
BEGIN
    CREATE DATABASE [EGX_Warehouse];
END
GO

USE [EGX_Warehouse];
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'stg')  EXEC('CREATE SCHEMA stg');
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold') EXEC('CREATE SCHEMA gold');
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'ops')  EXEC('CREATE SCHEMA ops');
GO

IF OBJECT_ID('ops.pipeline_runs', 'U') IS NULL
CREATE TABLE ops.pipeline_runs (
    run_id       BIGINT IDENTITY(1,1) PRIMARY KEY,
    step_name    NVARCHAR(100) NOT NULL,
    status       NVARCHAR(20)  NOT NULL,
    rows_read    BIGINT        NULL,
    rows_written BIGINT        NULL,
    message      NVARCHAR(500) NULL,
    started_at   DATETIME2(0)  NOT NULL DEFAULT SYSDATETIME(),
    finished_at  DATETIME2(0)  NULL
);
GO

IF OBJECT_ID('gold.dim_exchange', 'U') IS NULL
CREATE TABLE gold.dim_exchange (
    exchange_key  INT IDENTITY(1,1) PRIMARY KEY,
    exchange_code NVARCHAR(10)  NOT NULL UNIQUE,
    exchange_name NVARCHAR(100) NOT NULL,
    country_code  CHAR(2)       NOT NULL,
    currency_code CHAR(3)       NOT NULL,
    timezone      NVARCHAR(40)  NOT NULL,
    weekend_days  NVARCHAR(20)  NOT NULL,
    yahoo_suffix  NVARCHAR(10)  NOT NULL
);

IF OBJECT_ID('gold.dim_source', 'U') IS NULL
CREATE TABLE gold.dim_source (
    source_key   INT IDENTITY(1,1) PRIMARY KEY,
    source_code  NVARCHAR(30)  NOT NULL UNIQUE,
    source_name  NVARCHAR(100) NOT NULL,
    source_type  NVARCHAR(20)  NOT NULL,
    provider     NVARCHAR(60)  NOT NULL,
    landing_path NVARCHAR(200) NOT NULL,
    licence_note NVARCHAR(300) NULL, 
    is_active    BIT           NOT NULL DEFAULT 1
);

IF OBJECT_ID('gold.dim_date', 'U') IS NULL
CREATE TABLE gold.dim_date (
    date_key           INT          PRIMARY KEY, 
    full_date          DATE         NOT NULL UNIQUE,
    [year]             SMALLINT     NOT NULL,
    [quarter]          TINYINT      NOT NULL,
    [month]            TINYINT      NOT NULL,
    month_name         NVARCHAR(10) NOT NULL,
    year_month         CHAR(7)      NOT NULL,
    week_of_year       TINYINT      NOT NULL,
    day_of_month       TINYINT      NOT NULL,
    day_of_week        TINYINT      NOT NULL,
    day_name           NVARCHAR(10) NOT NULL,
    is_weekend         BIT          NOT NULL DEFAULT 0,
    is_month_end       BIT          NOT NULL DEFAULT 0,
    is_market_holiday  BIT          NOT NULL DEFAULT 0, 
    is_egx_trading_day BIT          NOT NULL DEFAULT 1  
);

IF OBJECT_ID('gold.dim_symbol', 'U') IS NULL
CREATE TABLE gold.dim_symbol (
    symbol_key         INT IDENTITY(1,1) PRIMARY KEY,
    symbol             NVARCHAR(20)  NOT NULL UNIQUE,
    yahoo_ticker       NVARCHAR(20)  NULL, 
    isin               CHAR(12)      NULL,
    company_name       NVARCHAR(150) NULL,
    asset_class        NVARCHAR(20)  NOT NULL DEFAULT 'Equities',
    instrument_type    NVARCHAR(20)  NOT NULL DEFAULT 'stock',
    share_class        NVARCHAR(20)  NULL,
    currency_code      CHAR(3)       NOT NULL DEFAULT 'EGP',
    is_primary_listing BIT           NOT NULL DEFAULT 0,
    exchange_key       INT           NOT NULL REFERENCES gold.dim_exchange(exchange_key),
    logo_id            NVARCHAR(150) NULL,
    metadata_provider  NVARCHAR(20)  NULL,
    found_by_isin      BIT           NOT NULL DEFAULT 0,
    has_metadata       BIT           NOT NULL DEFAULT 0,
    has_price_data     BIT           NOT NULL DEFAULT 0,
    updated_at         DATETIME2(0)  NOT NULL DEFAULT SYSDATETIME()
);
GO


IF OBJECT_ID('gold.fact_daily_price', 'U') IS NULL
CREATE TABLE gold.fact_daily_price (
    symbol_key        INT           NOT NULL REFERENCES gold.dim_symbol(symbol_key),
    date_key          INT           NOT NULL REFERENCES gold.dim_date(date_key),
    source_key        INT           NOT NULL REFERENCES gold.dim_source(source_key),
    open_price        DECIMAL(18,4) NULL,
    high_price        DECIMAL(18,4) NULL,
    low_price         DECIMAL(18,4) NULL,
    close_price       DECIMAL(18,4) NOT NULL,
    adj_close_price   DECIMAL(18,4) NULL,
    volume            BIGINT        NOT NULL DEFAULT 0,
    trade_value       DECIMAL(22,3) NOT NULL DEFAULT 0, 
    dividend_amount   DECIMAL(18,6) NOT NULL DEFAULT 0,
    stock_split_ratio DECIMAL(12,6) NOT NULL DEFAULT 0,
    capital_gains     DECIMAL(18,6) NOT NULL DEFAULT 0,
    is_traded         BIT           NOT NULL DEFAULT 1,
    dq_flags          NVARCHAR(200) NOT NULL DEFAULT 'OK',
    fetched_at_utc    DATETIME2(0)  NULL,
    source_file       NVARCHAR(200) NULL,
    mongo_doc_id      CHAR(24)      NULL, 
    run_id            BIGINT        NOT NULL REFERENCES ops.pipeline_runs(run_id),
    loaded_at         DATETIME2(0)  NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_fact_daily_price PRIMARY KEY (symbol_key, date_key, source_key)
);

IF OBJECT_ID('gold.fact_dividend_snapshot', 'U') IS NULL
CREATE TABLE gold.fact_dividend_snapshot (
    symbol_key         INT           NOT NULL REFERENCES gold.dim_symbol(symbol_key),
    snapshot_date      DATE          NOT NULL,
    source_key         INT           NOT NULL REFERENCES gold.dim_source(source_key),
    dividend_type      NVARCHAR(20)  NULL, 
    amount_recent      DECIMAL(18,6) NULL, 
    ex_date_recent     DATE          NULL, 
    amount_upcoming    DECIMAL(18,6) NULL, 
    ex_date_upcoming   DATE          NULL, 
    yield_pct          DECIMAL(9,4)  NOT NULL DEFAULT 0,
    run_id             BIGINT        NOT NULL REFERENCES ops.pipeline_runs(run_id),
    CONSTRAINT PK_fact_dividend_snapshot PRIMARY KEY (symbol_key, snapshot_date)
);
GO