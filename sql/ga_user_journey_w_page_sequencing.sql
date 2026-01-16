-- ============================================================================
-- GA4 USER JOURNEY ANALYSIS WITH PAGE SEQUENCING
-- ============================================================================
-- Source data: GA4 preprocessed data (ga4_preprocessed table)
-- ============================================================================

-- ============================================================================
-- STEP 1: Base data extraction and windowed metrics
-- ============================================================================
WITH STEP1 AS (
    SELECT
        session_id,
        trace_time,
        page_template,
        url,
        
        -- Traffic source fields
        traffic_source,
        traffic_source_grouped,
        traffic_medium,
        traffic_campaign,
        
        -- Entry traffic source
        FIRST_VALUE(traffic_source_grouped) OVER (
            PARTITION BY session_id ORDER BY trace_time
        ) AS entry_page_traffic_source,
        
        device_type,
        page_type,
        
        -- Category fields
        product_category,
        product_category_full,
        
        -- Conversion flag
        is_conversion,
        
        -- Generate a 1-based index for events within each session ordered by time
        ROW_NUMBER() OVER (
            PARTITION BY session_id ORDER BY trace_time ASC
        ) AS event_seq_num,
            
        -- Grab the URL of the previous event to detect refreshes
        COALESCE(
            LAG(url) OVER (PARTITION BY session_id ORDER BY trace_time ASC), 
            'N/A'
        ) AS url_lag,
            
        -- Navigation Context: Previous, Next, and 2-steps-back page templates
        LAG(page_template, 1, 'N/A') OVER (
            PARTITION BY session_id ORDER BY trace_time
        ) AS prev_page_template,
        
        LEAD(page_template, 1, 'N/A') OVER (
            PARTITION BY session_id ORDER BY trace_time
        ) AS next_page_template,
        
        LAG(page_template, 2, 'N/A') OVER (
            PARTITION BY session_id ORDER BY trace_time
        ) AS two_ahead_page_template,
            
        -- Session Boundaries: First and Last templates visited
        FIRST_VALUE(page_template) OVER (
            PARTITION BY session_id ORDER BY trace_time
        ) AS first_page_template,
        
        LAST_VALUE(page_template) OVER (
            PARTITION BY session_id ORDER BY trace_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_page_template,    
            
        -- Total depth of session (event count) calculated on every row
        COUNT(page_template) OVER (
            PARTITION BY session_id
        ) AS session_event_cnt,                
            
        url_host,
        url_path,
        
        -- URL parameters extraction (for compatibility)
        REGEXP_EXTRACT_ALL(url, r'[?&]((?:[^=]+)=(?:[^&]+))') AS param_pairs,
            
        -- Entry page traffic source
        FIRST_VALUE(traffic_source) OVER (
            PARTITION BY session_id ORDER BY trace_time
        ) AS entry_page_traffic_source_raw,
        
        -- Entry page traffic medium
        FIRST_VALUE(traffic_medium) OVER (
            PARTITION BY session_id ORDER BY trace_time
        ) AS entry_page_traffic_medium

    FROM 
        `{{PROJECT_ID}}.{{DATASET_ID}}.ga_preprocessed`
    
    -- Remove consecutive refreshes (same URL as previous event)
    QUALIFY
        LAG(url, 1, 'N/A') OVER (PARTITION BY session_id ORDER BY trace_time ASC) != url
)

-- ============================================================================
-- STEP 2: URL Parameter Parsing
-- ============================================================================
, STEP2 AS (
    SELECT
        s1.session_id,
        s1.trace_time,
        s1.page_template,
        s1.url,
        s1.traffic_source,
        s1.traffic_source_grouped,
        s1.traffic_medium,
        s1.device_type,
        s1.page_type,
        s1.product_category,
        s1.product_category_full,
        s1.is_conversion,
        s1.event_seq_num,
        s1.url_lag,
        s1.url_host,
        s1.url_path,
        s1.prev_page_template,
        s1.next_page_template,
        s1.two_ahead_page_template,
        s1.entry_page_traffic_source,
        s1.last_page_template,
        s1.session_event_cnt,
        s1.entry_page_traffic_source_raw,
        s1.entry_page_traffic_medium,
        
        -- Parse URL parameters into structured array
        ARRAY_AGG(
            STRUCT(
                REGEXP_EXTRACT(param_pair_element, r'([^=]+)') AS param_name,
                REGEXP_REPLACE(
                    REGEXP_EXTRACT(param_pair_element, r'[^=]+=([^&]+)'),
                    r'\+',
                    ' '
                ) AS param_value
            )
            IGNORE NULLS
        ) AS parsed_params_array
    FROM
        STEP1 AS s1
        LEFT JOIN UNNEST(s1.param_pairs) AS param_pair_element ON TRUE
    GROUP BY
        s1.session_id,
        s1.trace_time,
        s1.page_template,
        s1.url,
        s1.traffic_source,
        s1.traffic_source_grouped,
        s1.traffic_medium,
        s1.device_type,
        s1.page_type,
        s1.product_category,
        s1.product_category_full,
        s1.is_conversion,
        s1.event_seq_num,
        s1.url_lag,
        s1.url_host,
        s1.url_path,
        s1.prev_page_template,
        s1.next_page_template,
        s1.two_ahead_page_template,
        s1.entry_page_traffic_source,
        s1.last_page_template,
        s1.session_event_cnt,
        s1.entry_page_traffic_source_raw,
        s1.entry_page_traffic_medium
)

-- ============================================================================
-- STEP 3: Table Flattening (one row per parameter)
-- ============================================================================
, STEP3 AS (
    SELECT
        s2.session_id,
        s2.trace_time,
        s2.page_template,
        s2.url,
        s2.traffic_source,
        s2.traffic_source_grouped,
        s2.traffic_medium,
        s2.page_type,
        s2.product_category,
        s2.product_category_full,
        s2.is_conversion,
        s2.event_seq_num,
        s2.url_lag,
        s2.url_host,
        s2.url_path,
        s2.prev_page_template,
        s2.next_page_template,
        s2.two_ahead_page_template,
        s2.entry_page_traffic_source,
        s2.last_page_template,
        s2.session_event_cnt,
        s2.entry_page_traffic_source_raw,
        s2.entry_page_traffic_medium,
        
        -- Expand struct array into columns
        param_data.param_name,
        param_data.param_value
    FROM
        STEP2 AS s2
        LEFT JOIN UNNEST(s2.parsed_params_array) AS param_data ON TRUE
)

-- ============================================================================
-- Session Aggregation: Session-level metrics
-- ============================================================================
, session_agg AS (
    SELECT 
        session_id, 
        COUNT(1) AS event_cnt,
        
        -- Full user journey path as string
        STRING_AGG(page_template, ' > ' ORDER BY event_seq_num ASC) AS journey_fingerprint_str,
        
        -- First 5 steps of journey only
        ARRAY_TO_STRING(
            ARRAY_AGG(page_template ORDER BY event_seq_num ASC LIMIT 5), 
            ' > '
        ) AS journey_fingerprint_first_five_str,
        
        -- Entry page attributes
        ARRAY_AGG(page_template ORDER BY trace_time ASC LIMIT 1)[SAFE_OFFSET(0)] AS entry_page_template,
        ARRAY_AGG(entry_page_traffic_source_raw ORDER BY trace_time ASC LIMIT 1)[SAFE_OFFSET(0)] AS entry_page_traffic_source_session_lvl,
        ARRAY_AGG(traffic_source ORDER BY trace_time ASC LIMIT 1)[SAFE_OFFSET(0)] AS entry_page_source,
        ARRAY_AGG(traffic_medium ORDER BY trace_time ASC LIMIT 1)[SAFE_OFFSET(0)] AS entry_page_medium,
        
        -- Primary device type (majority vote)
        APPROX_TOP_COUNT(device_type, 1)[SAFE_OFFSET(0)].value AS primary_device_type,
        
        -- Primary traffic source (majority vote)
        APPROX_TOP_COUNT(traffic_source_grouped, 1)[SAFE_OFFSET(0)].value AS primary_traffic_source,
        
        -- Behavior Flags
        LOGICAL_OR(LOWER(page_template) = 'productdetail') AS saw_product_detail,
        
        -- had_conversion
        LOGICAL_OR(is_conversion = TRUE) AS had_conversion, 
        SUM(CASE WHEN is_conversion = TRUE THEN 1 ELSE 0 END) AS conversion_cnt
        
    FROM 
        step1
    WHERE 
        NOT url_lag = url
    GROUP BY
        session_id
)

-- ============================================================================
-- Bi-grams: Transition Analysis (Step A -> Step B)
-- ============================================================================
, bi_grams AS (
    SELECT
        s1.prev_page_template || ' > ' || s1.page_template AS page_template_transition,
        COUNT(1) AS transitions_count
    FROM
        step1 s1
    WHERE
        session_event_cnt >= 2
        AND NOT url_lag = url
    GROUP BY 1
    ORDER BY 2 DESC
)

-- ============================================================================
-- Tri-grams: Complex Flow Analysis (Step A -> Step B -> Step C)
-- ============================================================================
, tri_grams AS (
    SELECT
        s1.two_ahead_page_template 
            || ' > ' 
            || s1.prev_page_template 
            || ' > ' 
            || CASE WHEN 
                -- If traffic source exists AND is NOT internal/direct, flag as external re-entry
                (s1.traffic_source IS NOT NULL AND s1.traffic_source NOT IN ('(direct)', '(not set)'))
                AND s1.traffic_source_grouped != 'direct'
                THEN '(external) ' || s1.page_template 
                ELSE s1.page_template
            END AS page_template_transition,
        COUNT(1) AS transitions_count
    FROM
        step1 s1
    WHERE
        session_event_cnt >= 3
        AND NOT url_lag = url
    GROUP BY 1
)

-- ============================================================================
-- Final Enrichment: Master Dataset
-- ============================================================================
, final_enrichment AS (
    SELECT 
        s3.*,
        sa.event_cnt,
        sa.journey_fingerprint_str,
        sa.journey_fingerprint_first_five_str,
        sa.entry_page_template,
        sa.entry_page_source,
        sa.entry_page_medium,
        sa.primary_device_type,
        sa.primary_traffic_source,
        sa.saw_product_detail,
        sa.had_conversion,
        sa.conversion_cnt
    FROM 
        step3 s3
        LEFT OUTER JOIN session_agg sa ON s3.session_id = sa.session_id
    ORDER BY 
        trace_time ASC 
)

-- ============================================================================
-- Session Sources: Attribution Logic for Google
-- ============================================================================
, session_sources AS (
    SELECT 
        session_id,
        -- Google via traffic_source field
        CASE WHEN SUM(
            CASE WHEN LOWER(traffic_source) LIKE '%google%' THEN 1 ELSE 0 END 
        ) > 0 THEN 1 ELSE 0 END AS google_source_flag,
        
        -- Google via traffic_source_grouped
        CASE WHEN SUM(
            CASE WHEN traffic_source_grouped = 'google' THEN 1 ELSE 0 END 
        ) > 0 THEN 1 ELSE 0 END AS google_grouped_flag,
        
        -- Organic Google traffic
        CASE WHEN SUM(
            CASE WHEN traffic_source_grouped = 'google' AND traffic_medium = 'organic' THEN 1 ELSE 0 END 
        ) > 0 THEN 1 ELSE 0 END AS google_organic_flag,
        
        -- Paid Google traffic (CPC)
        CASE WHEN SUM(
            CASE WHEN traffic_source_grouped = 'google' AND traffic_medium = 'cpc' THEN 1 ELSE 0 END 
        ) > 0 THEN 1 ELSE 0 END AS google_cpc_flag,
        
        -- Google on first event only (entry attribution)
        CASE WHEN SUM(
            CASE WHEN (event_seq_num = 1) AND traffic_source_grouped = 'google' THEN 1 ELSE 0 END 
        ) > 0 THEN 1 ELSE 0 END AS google_first_event_flag
    FROM 
        final_enrichment
    GROUP BY
        session_id
)

-- ============================================================================
-- Frequent Product Detail Journeys
-- ============================================================================
, frequent_product_details AS ( 
    SELECT
        journey_fingerprint_str,
        COUNT(1) AS cnt
    FROM   
        session_agg
    GROUP BY
        1
    ORDER BY
        cnt DESC
)

-- ============================================================================
-- Entry Page Traffic Source Analysis
-- ============================================================================
, entry_page_source_analysis AS ( 
    SELECT 
        entry_page_source,
        entry_page_medium,
        COUNT(1) AS cnt
    FROM 
        session_agg
    GROUP BY 1, 2
    ORDER BY cnt DESC
)

-- ============================================================================
-- Category Frequency: Product detail views by category
-- ============================================================================
, category_frequency AS (     
    SELECT
        product_category,
        COUNT(1) AS cnt
    FROM
        final_enrichment
    WHERE 
        page_template = 'ProductDetail'
        AND NOT url_lag = url
    GROUP BY 
        1
)

-- ============================================================================
-- Frequent Product Detail Journeys by Source
-- ============================================================================
, frequent_product_details_by_source AS (
    SELECT
        journey_fingerprint_str,
        COALESCE(
            entry_page_traffic_source_session_lvl, 
            primary_traffic_source
        ) AS entry_page_traffic_source,
        COUNT(1) AS cnt
    FROM   
        session_agg
    GROUP BY
        1, 2
    HAVING journey_fingerprint_str = 'ProductDetail'
    ORDER BY
        cnt DESC
)

-- ============================================================================
-- Traffic by Source (Overall attribution)
-- ============================================================================
, traffic_by_source AS (
    SELECT  
        COALESCE(traffic_source, primary_traffic_source) AS primary_source,
        traffic_medium,
        COUNT(DISTINCT fe.session_id) AS cnt 
    FROM 
        final_enrichment fe 
    GROUP BY 
        1, 2
)

-- ============================================================================
-- Traffic to Entry Page by Source (First Touch only)
-- ============================================================================
, traffic_to_entry_page_by_source AS (
    SELECT  
        COALESCE(fe.entry_page_traffic_source, fe.primary_traffic_source) AS primary_source,
        fe.entry_page_medium,
        COUNT(DISTINCT fe.session_id) AS cnt 
    FROM 
        final_enrichment fe 
    WHERE 
        fe.event_seq_num = 1
    GROUP BY 
        1, 2
)

-- ============================================================================
-- Google Traffic Aggregation
-- ============================================================================
, google_traffic AS (
    SELECT 
        SUM(google_source_flag) AS google_source_sessions_cnt,
        SUM(google_grouped_flag) AS google_grouped_sessions_cnt,
        SUM(google_organic_flag) AS google_organic_cnt,
        SUM(google_cpc_flag) AS google_cpc_cnt,
        SUM(google_first_event_flag) AS google_first_event_cnt
    FROM session_sources
)

-- ============================================================================
-- Session Count by Journey
-- ============================================================================
, session_count_by_journey AS (     
    SELECT 
        journey_fingerprint_str,
        COUNT(DISTINCT session_id) AS cnt 
    FROM 
        final_enrichment 
    GROUP BY 1 
)

-- ============================================================================
-- Sample Session (Debug)
-- ============================================================================
, sample_session AS (
    SELECT 
        TIMESTAMP_MILLIS(CAST(s3.trace_time AS INT64)) AS trace_timestamp_millis,
        * 
    FROM 
        STEP3 s3
    WHERE 
        session_id = (SELECT session_id FROM STEP3 LIMIT 1)  -- Pick first available session
)

-- ============================================================================
-- OUTPUT QUERIES (uncomment one to run)
-- ============================================================================

    -- select count(*) from final_enrichment;
    -- select * from bi_grams ORDER BY 2 DESC;
    SELECT * FROM tri_grams ORDER BY 2 DESC;
    -- select * from entry_page_source_analysis ORDER BY cnt DESC;
    -- select *, cnt/SUM(cnt) OVER () AS cnt_pct FROM category_frequency ORDER BY cnt DESC LIMIT 100;
    -- select * from frequent_product_details_by_source;
    -- select * from traffic_to_entry_page_by_source ORDER BY cnt DESC;
    -- select * from traffic_by_source ORDER BY cnt DESC;
    -- select * from session_count_by_journey ORDER BY cnt DESC;
    -- select * from session_sources LIMIT 100;
    -- select * from google_traffic;
    -- select * from sample_session ORDER BY event_seq_num ASC;