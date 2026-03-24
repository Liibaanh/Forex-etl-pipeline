-- =============================================================================
-- Fair Value Gap (FVG) Analysis Queries — EUR/USD
-- Target: Databricks SQL endpoint / local Delta tables
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. All detected FVGs ordered by size
--    Largest gaps are most significant in ICT theory
-- -----------------------------------------------------------------------------
SELECT
    date,
    fvg_type,
    fvg_high,
    fvg_low,
    ABS(fvg_size_pips)          AS fvg_size_pips,
    is_filled,
    CASE
        WHEN is_filled = 1 THEN 'Filled'
        ELSE 'Active'
    END                         AS status
FROM gold.fvg_eurusd
ORDER BY ABS(fvg_size_pips) DESC;


-- -----------------------------------------------------------------------------
-- 2. Active (unfilled) FVGs — current trade zones
--    These are the zones price is likely to return to
-- -----------------------------------------------------------------------------
SELECT
    date,
    fvg_type,
    fvg_high,
    fvg_low,
    ABS(fvg_size_pips)          AS fvg_size_pips
FROM gold.fvg_eurusd
WHERE is_filled = 0
ORDER BY date DESC;


-- -----------------------------------------------------------------------------
-- 3. FVG fill rate by type
--    How often does price return to fill bullish vs bearish gaps?
--    High fill rate = concept is working on this instrument
-- -----------------------------------------------------------------------------
SELECT
    fvg_type,
    COUNT(*)                                        AS total_fvgs,
    SUM(is_filled)                                  AS filled_count,
    COUNT(*) - SUM(is_filled)                       AS unfilled_count,
    ROUND(SUM(is_filled) * 100.0 / COUNT(*), 2)    AS fill_rate_pct
FROM gold.fvg_eurusd
GROUP BY fvg_type
ORDER BY fill_rate_pct DESC;


-- -----------------------------------------------------------------------------
-- 4. Monthly FVG activity
--    Which months have the most FVG formations?
--    High activity months = high volatility periods
-- -----------------------------------------------------------------------------
SELECT
    DATE_TRUNC('month', date)   AS month,
    COUNT(*)                    AS total_fvgs,
    SUM(CASE WHEN fvg_type = 'bullish' THEN 1 ELSE 0 END) AS bullish_fvgs,
    SUM(CASE WHEN fvg_type = 'bearish' THEN 1 ELSE 0 END) AS bearish_fvgs,
    ROUND(AVG(ABS(fvg_size_pips)), 2)               AS avg_size_pips,
    ROUND(MAX(ABS(fvg_size_pips)), 2)               AS max_size_pips
FROM gold.fvg_eurusd
GROUP BY DATE_TRUNC('month', date)
ORDER BY month DESC;


-- -----------------------------------------------------------------------------
-- 5. Rolling 20-day volatility alongside FVG zones
--    High volatility periods often produce larger FVGs
--    Combines Silver and Gold layers in one query
-- -----------------------------------------------------------------------------
WITH volatility AS (
    SELECT
        date,
        daily_range,
        AVG(daily_range) OVER (
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )                       AS volatility_20d,
        daily_return
    FROM silver.ohlc_eurusd
),
fvg_dates AS (
    SELECT
        date,
        fvg_type,
        ABS(fvg_size_pips)      AS fvg_size_pips
    FROM gold.fvg_eurusd
)
SELECT
    v.date,
    v.daily_range,
    ROUND(v.volatility_20d, 5)  AS volatility_20d,
    v.daily_return,
    f.fvg_type,
    f.fvg_size_pips
FROM volatility v
LEFT JOIN fvg_dates f ON v.date = f.date
ORDER BY v.date DESC;


-- -----------------------------------------------------------------------------
-- 6. Largest single-day moves — potential FVG trigger days
--    Big daily returns often create FVGs the following session
-- -----------------------------------------------------------------------------
SELECT
    date,
    open,
    high,
    low,
    close,
    daily_range,
    daily_return,
    RANK() OVER (
        ORDER BY ABS(daily_return) DESC
    )                           AS move_rank
FROM silver.ohlc_eurusd
QUALIFY move_rank <= 10
ORDER BY ABS(daily_return) DESC;