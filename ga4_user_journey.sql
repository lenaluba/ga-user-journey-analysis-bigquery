-- ============================================================================
-- GA USER JOURNEY ANALYSIS
-- ============================================================================
-- Source data: GA preprocessed data (from ga_data_preprocessing.sql)
-- ============================================================================

-- ============================================================================
-- DATA PREPROCESSING & SESSION AGGREGATION
-- ============================================================================

-- ============================================================================
-- Preprocessing: traffic source consolidation, session-level first values
-- ============================================================================
WITH PREPROCESSED_DATA AS (
  SELECT 
    *,
    
    -- First traffic source per session
    FIRST_VALUE(traffic_source_grouped) OVER (
      PARTITION BY session_id 
      ORDER BY trace_time ASC 
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS traffic_source_first,

    -- First traffic medium per session (for channel analysis)
    FIRST_VALUE(traffic_medium) OVER (
      PARTITION BY session_id 
      ORDER BY trace_time ASC 
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS traffic_medium_first,

    -- First traffic campaign per session
    FIRST_VALUE(traffic_campaign) OVER (
      PARTITION BY session_id 
      ORDER BY trace_time ASC 
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS traffic_campaign_first

  FROM `temp-481115.ga_user_journey_analysis_case.ga_preprocessed` 
)

-- ============================================================================
-- Session aggregation: journey paths, duration, device, entry page, conversion
-- ============================================================================
, SESSION_AGG AS (
  SELECT 
    session_id,
    STRING_AGG(page_template, ' -> ' ORDER BY trace_time ASC) AS journey_path,
    COUNT(*) AS journey_steps,
    (MAX(trace_time) - MIN(trace_time))/1000 AS journey_duration_seconds,
    APPROX_TOP_COUNT(device_type, 1)[SAFE_OFFSET(0)].value AS primary_device_type,
    ARRAY_AGG(page_template ORDER BY trace_time ASC LIMIT 1)[SAFE_OFFSET(0)] AS entry_page_template,
    
    -- Traffic source fields
    ANY_VALUE(traffic_source_first) AS traffic_source_first,
    ANY_VALUE(traffic_medium_first) AS traffic_medium_first,
    ANY_VALUE(traffic_campaign_first) AS traffic_campaign_first,
    
    -- Product detail view flag
    LOGICAL_OR(page_template = 'ProductDetail') AS saw_product_detail,
    
    -- Conversion flag
    LOGICAL_OR(is_conversion = TRUE) AS has_conversion
    
  FROM PREPROCESSED_DATA
  GROUP BY session_id
)

-- ============================================================================
-- ANALYSIS QUERIES
-- ============================================================================

-- ============================================================================
-- Q1: Journey patterns
-- ============================================================================

-- ### Events by page template
-- SELECT page_template, COUNT(*) as cnt
-- FROM PREPROCESSED_DATA
-- GROUP BY page_template
-- ORDER BY cnt DESC
-- LIMIT 50
-- ;

-- ### Channel analysis: source + medium combination
-- SELECT 
--   CONCAT(
--     COALESCE(traffic_source_first, '(direct)'), 
--     ' / ', 
--     COALESCE(traffic_medium_first, '(none)')
--   ) AS source_medium,
--   COUNT(*) AS session_count
-- FROM SESSION_AGG
-- WHERE journey_path = 'ProductDetail'
-- GROUP BY source_medium
-- ORDER BY session_count DESC
-- ;

-- ### Traffic medium frequency
-- SELECT traffic_medium_first, COUNT(*) AS cnt
-- FROM SESSION_AGG
-- GROUP BY traffic_medium_first
-- ORDER BY cnt DESC
-- ;

-- ### Traffic source frequency
-- SELECT traffic_source_first, COUNT(*) AS cnt
-- FROM SESSION_AGG
-- GROUP BY traffic_source_first
-- ORDER BY cnt DESC
-- ;

-- ### Journey path frequency by device
-- , JOURNEY_FREQUENCY AS (
--   SELECT 
--     journey_path,
--     COUNT(DISTINCT session_id) AS session_count,
--     AVG(journey_duration_seconds) AS avg_duration_sec,
--     ANY_VALUE(journey_steps) AS steps,
--     -- Device breakdown (desktop, mobile, tablet)
--     COUNTIF(LOWER(primary_device_type) = 'desktop') AS desktop_sessions,
--     COUNTIF(LOWER(primary_device_type) = 'mobile') AS mobile_sessions,
--     COUNTIF(LOWER(primary_device_type) = 'tablet') AS tablet_sessions,
--   FROM SESSION_AGG
--   GROUP BY journey_path
--   ORDER BY session_count DESC
-- )

-- SELECT *
-- FROM JOURNEY_FREQUENCY
-- ORDER BY session_count DESC
-- ;

-- ============================================================================
-- Q2: Traffic source analysis
-- ============================================================================

-- ### Sessions by traffic source type
-- SELECT 
--   CASE 
--     WHEN LOWER(traffic_source_grouped) = 'google' THEN 'google-touched' 
--     WHEN traffic_source_grouped IS NULL OR traffic_source_grouped = 'direct' THEN 'direct'
--     ELSE 'other source' 
--   END AS source_type,  
--   COUNT(DISTINCT session_id) as cnt
-- FROM PREPROCESSED_DATA
-- GROUP BY source_type
-- ORDER BY cnt DESC
-- ;

-- ### Traffic source frequency
-- SELECT traffic_source_first, COUNT(*) AS cnt
-- FROM SESSION_AGG
-- GROUP BY traffic_source_first
-- ORDER BY cnt DESC
-- ;

-- ### Bounce rate by traffic source
-- SELECT 
--   COUNT(*) AS source_count,
--   COUNTIF(journey_steps = 1) AS bounce,
--   COUNTIF(journey_steps > 1) AS not_bounce
-- FROM SESSION_AGG
-- WHERE traffic_source_first <> 'google'
-- ;

-- -- ### Conversion rate by traffic source
-- SELECT 
--   COUNT(*) AS source_count,
--   COUNTIF(has_conversion = FALSE) AS no_conversion,
--   COUNTIF(has_conversion = TRUE) AS has_conversion
-- FROM SESSION_AGG
-- -- WHERE traffic_source_first <> 'google'
-- ;

-- ============================================================================
-- Q3: Homepage -> ProductDetail conversion
-- ============================================================================

-- ### Landing page frequency
-- SELECT page_template, COUNT(*) as cnt
-- FROM PREPROCESSED_DATA
-- WHERE event_seq_num = 1
-- GROUP BY page_template
-- ORDER BY cnt DESC
-- ;

-- ### Homepage sessions: Product detail view rate by device
-- SELECT
--   CASE 
--     WHEN saw_product_detail = TRUE THEN 'homepage_sessions_with_product_detail'
--     WHEN saw_product_detail = FALSE AND journey_steps = 1 THEN 'homepage_sessions_bounce'
--     ELSE 'homepage_sessions_without_product_detail'
--   END AS session_type,
--   COUNT(*) AS total_sessions,
--   -- Device breakdown
--   COUNTIF(LOWER(primary_device_type) = 'desktop') AS desktop_sessions,
--   COUNTIF(LOWER(primary_device_type) = 'mobile') AS mobile_sessions,
--   COUNTIF(LOWER(primary_device_type) = 'tablet') AS tablet_sessions,
--   -- COUNTIF(primary_device_type IS NULL) AS null_sessions,
--   -- Conversion metrics
--   COUNTIF(has_conversion = TRUE) AS has_conversion,
--   COUNTIF(has_conversion = FALSE) AS no_conversion
-- FROM SESSION_AGG
-- WHERE
--   entry_page_template = 'Homepage'
-- GROUP BY session_type
-- ORDER BY session_type DESC
-- ;

-- ### Category landing sessions: Product detail view rate by device
-- ### Alternative analysis for category entry pages
-- SELECT
--   CASE 
--     WHEN saw_product_detail = TRUE THEN 'category_sessions_with_product_detail'
--     WHEN saw_product_detail = FALSE AND journey_steps = 1 THEN 'category_sessions_bounce'
--     ELSE 'category_sessions_without_product_detail'
--   END AS session_type,
--   COUNT(*) AS total_sessions,
--   COUNTIF(LOWER(primary_device_type) = 'desktop') AS desktop_sessions,
--   COUNTIF(LOWER(primary_device_type) = 'mobile') AS mobile_sessions,
--   COUNTIF(LOWER(primary_device_type) = 'tablet') AS tablet_sessions,
--   COUNTIF(has_conversion = TRUE) AS has_conversion,
--   COUNTIF(has_conversion = FALSE) AS no_conversion
-- FROM SESSION_AGG
-- WHERE
--   entry_page_template IN ('CategoryPage', 'TopCategory')
-- GROUP BY session_type
-- ORDER BY session_type DESC
-- ;

-- ============================================================================
-- Q4: Product detail impressions by category
-- ============================================================================

-- SELECT 
--   product_category,
--   COUNT(*) AS category_impressions
-- FROM PREPROCESSED_DATA
-- WHERE page_template = 'ProductDetail'
-- GROUP BY product_category
-- ORDER BY category_impressions DESC
-- ;

-- ============================================================================
-- Q5: Traffic medium analysis
-- ============================================================================

-- ### Sessions by traffic medium
-- SELECT 
--   traffic_medium_first,
--   COUNT(*) AS session_count,
--   COUNTIF(journey_steps = 1) AS bounce_count,
--   COUNTIF(has_conversion = TRUE) AS conversion_count,
--   ROUND(COUNTIF(journey_steps = 1) * 100.0 / COUNT(*), 2) AS bounce_rate_pct,
--   ROUND(COUNTIF(has_conversion = TRUE) * 100.0 / COUNT(*), 2) AS conversion_rate_pct
-- FROM SESSION_AGG
-- GROUP BY traffic_medium_first
-- ORDER BY session_count DESC
-- ;

### Paid vs Organic comparison
SELECT 
  CASE 
    WHEN traffic_medium_first = 'cpc' THEN 'Paid Search'
    WHEN traffic_medium_first = 'organic' THEN 'Organic Search'
    WHEN traffic_medium_first IN ('referral') THEN 'Referral'
    WHEN traffic_medium_first = '(none)' OR traffic_medium_first IS NULL THEN 'Direct'
    ELSE 'Other'
  END AS channel_group,
  COUNT(*) AS session_count,
  AVG(journey_steps) AS avg_journey_steps,
  AVG(journey_duration_seconds) AS avg_duration_sec,
  ROUND(COUNTIF(has_conversion = TRUE) * 100.0 / COUNT(*), 2) AS conversion_rate_pct
FROM SESSION_AGG
GROUP BY channel_group
ORDER BY session_count DESC
;
