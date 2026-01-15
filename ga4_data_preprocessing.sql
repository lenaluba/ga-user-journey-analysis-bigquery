-- ============================================================================
-- GA4 DATA PREPROCESSING (Adapted for User Journey Analysis)
-- ============================================================================
-- Source: bigquery-public-data.google_analytics_sample.ga_sessions_*
-- Purpose: Transform raw GA data into a structure compatible with journey analysis
-- Version: v3 - Fixed page template detection order and Homepage pattern
-- ============================================================================

SELECT
  -- ============================================================================
  -- Session & Event Identifiers
  -- ============================================================================
  CONCAT(fullVisitorId, '-', visitId) AS session_id,
  visitStartTime * 1000 + hits.time AS trace_time,
  hits.hitNumber AS event_seq_num,

  -- ============================================================================
  -- URL Fields
  -- ============================================================================
  hits.page.pagePath AS url_path,
  hits.page.hostname AS url_host,
  CONCAT('https://', hits.page.hostname, hits.page.pagePath) AS url,

  -- ============================================================================
  -- Device Information
  -- ============================================================================
  device.deviceCategory AS device_type,

  -- ============================================================================
  -- Page Type (Raw from GA)
  -- ============================================================================
  -- Values: PAGE, EVENT, TRANSACTION, ITEM, SOCIAL, EXCEPTION, TIMING
  hits.type AS page_type,

  -- ============================================================================
  -- Page Template (Derived from URL patterns)
  -- ============================================================================
  -- IMPORTANT: Order matters! Check specific keyword patterns BEFORE 
  -- path-depth patterns to avoid misclassification.
  -- 
  -- Classification order:
  -- 1. Static assets (file extensions)
  -- 2. Transactional pages (cart, checkout, confirmation)
  -- 3. Functional pages (search, quickview, store locator, account)
  -- 4. Content pages by URL depth (product detail → category → top category)
  -- 5. Homepage (exact matches only)
  -- 6. Fallback
  -- ============================================================================
  CASE 
    -- -------------------------------------------------------------------------
    -- 1. STATIC ASSETS (exclude from journey analysis)
    -- -------------------------------------------------------------------------
    WHEN REGEXP_CONTAINS(hits.page.pagePath, r'\.(axd|js|css|png|jpg|jpeg|gif|svg|ico|woff|woff2|ttf|eot)(\?|$)') 
      THEN 'Asset'
    
    -- -------------------------------------------------------------------------
    -- 2. TRANSACTIONAL PAGES (highest priority after assets)
    -- -------------------------------------------------------------------------
    -- Cart/Basket pages
    WHEN LOWER(hits.page.pagePath) LIKE '%basket%' 
      OR LOWER(hits.page.pagePath) LIKE '%cart%'
      THEN 'Cart'
    
    -- Checkout pages
    WHEN LOWER(hits.page.pagePath) LIKE '%checkout%'
      OR LOWER(hits.page.pagePath) LIKE '%payment%'
      OR LOWER(hits.page.pagePath) LIKE '%revieworder%'
      THEN 'Checkout'
    
    -- Order confirmation
    WHEN LOWER(hits.page.pagePath) LIKE '%ordercompleted%'
      OR LOWER(hits.page.pagePath) LIKE '%orderconfirm%'
      OR LOWER(hits.page.pagePath) LIKE '%order-confirm%'
      OR LOWER(hits.page.pagePath) LIKE '%thankyou%'
      OR LOWER(hits.page.pagePath) LIKE '%thank-you%'
      THEN 'OrderConfirmation'
    
    -- -------------------------------------------------------------------------
    -- 3. FUNCTIONAL PAGES
    -- -------------------------------------------------------------------------
    -- Search results
    WHEN LOWER(hits.page.pagePath) LIKE '%/search%'
      OR LOWER(hits.page.pagePath) LIKE '%?q=%'
      OR LOWER(hits.page.pagePath) LIKE '%&q=%'
      OR LOWER(hits.page.pagePath) LIKE '%asearch%'
      OR REGEXP_CONTAINS(LOWER(hits.page.pagePath), r'[?&](query|keyword|search)=')
      THEN 'SearchResults'
    
    -- Product quickview
    WHEN LOWER(hits.page.pagePath) LIKE '%quickview%'
      OR LOWER(hits.page.pagePath) LIKE '%quick-view%'
      THEN 'ProductQuickview'
    
    -- Store locator / physical store pages
    WHEN LOWER(hits.page.pagePath) LIKE '%storelocator%'
      OR LOWER(hits.page.pagePath) LIKE '%store-locator%'
      OR LOWER(hits.page.pagePath) LIKE '%findstore%'
      OR LOWER(hits.page.pagePath) LIKE '%find-store%'
      OR (LOWER(hits.page.pagePath) LIKE '%store%' AND LOWER(hits.page.pagePath) LIKE '%locator%')
      THEN 'StoreLocator'
    
    -- Sign in / Account pages
    WHEN LOWER(hits.page.pagePath) LIKE '%signin%'
      OR LOWER(hits.page.pagePath) LIKE '%sign-in%'
      OR LOWER(hits.page.pagePath) LIKE '%login%'
      OR LOWER(hits.page.pagePath) LIKE '%log-in%'
      OR LOWER(hits.page.pagePath) LIKE '%account%'
      OR LOWER(hits.page.pagePath) LIKE '%myaccount%'
      OR LOWER(hits.page.pagePath) LIKE '%my-account%'
      OR LOWER(hits.page.pagePath) LIKE '%register%'
      OR LOWER(hits.page.pagePath) LIKE '%signup%'
      THEN 'Account'
    
    -- -------------------------------------------------------------------------
    -- 4. CONTENT PAGES (by URL depth - check deepest first)
    -- -------------------------------------------------------------------------
    -- Product detail pages: 3+ path segments
    -- Pattern: /Category/Subcategory/ProductName or deeper
    -- Excludes paths containing transactional/functional keywords
    WHEN REGEXP_CONTAINS(hits.page.pagePath, r'^/[^/]+/[^/]+/[^/]+') 
      AND NOT REGEXP_CONTAINS(LOWER(hits.page.pagePath), r'(basket|cart|checkout|search|account|login|signin)')
      THEN 'ProductDetail'
    
    -- Category/Listing pages: exactly 2 path segments
    -- Pattern: /Category/Subcategory
    WHEN REGEXP_CONTAINS(hits.page.pagePath, r'^/[^/]+/[^/]+/?$')
      AND NOT REGEXP_CONTAINS(LOWER(hits.page.pagePath), r'(basket|cart|checkout|search|account|login|signin)')
      THEN 'CategoryPage'
    
    -- Top-level category pages: exactly 1 path segment (excluding homepage)
    -- Pattern: /Category
    WHEN REGEXP_CONTAINS(hits.page.pagePath, r'^/[^/]+/?$')
      AND NOT REGEXP_CONTAINS(LOWER(hits.page.pagePath), r'(basket|cart|checkout|search|account|login|signin)')
      AND LOWER(hits.page.pagePath) NOT IN ('/home', '/home/')
      THEN 'TopCategory'
    
    -- -------------------------------------------------------------------------
    -- 5. HOMEPAGE (exact matches only - checked AFTER category pages)
    -- -------------------------------------------------------------------------
    -- FIX: Removed LIKE '%home%' which incorrectly matched category URLs
    -- like /google+redesign/bags/backpacks/home
    WHEN hits.page.pagePath = '/' 
      OR hits.page.pagePath = '/home'
      OR hits.page.pagePath = '/home/'
      OR LOWER(hits.page.pagePath) = '/index.html'
      OR LOWER(hits.page.pagePath) = '/index.htm'
      OR LOWER(hits.page.pagePath) = '/default.aspx'
      THEN 'Homepage'
    
    -- -------------------------------------------------------------------------
    -- 6. FALLBACK
    -- -------------------------------------------------------------------------
    ELSE 'Other'
  END AS page_template,

  -- ============================================================================
  -- Traffic Source Fields (Consolidated)
  -- ============================================================================
  -- These fields replace both utm_source extraction and http_referer_apexdomain
  -- from the original idealo analysis
  trafficSource.source AS traffic_source,
  trafficSource.medium AS traffic_medium,
  trafficSource.campaign AS traffic_campaign,
  trafficSource.adContent AS traffic_ad_content,
  trafficSource.keyword AS traffic_keyword,
  trafficSource.adwordsClickInfo.gclid AS gclid,
  
  -- Derived: Clean traffic source for grouping
  CASE
    WHEN trafficSource.source IS NULL OR trafficSource.source IN ('(direct)', '(not set)') 
      THEN 'direct'
    WHEN LOWER(trafficSource.source) LIKE '%google%' 
      THEN 'google'
    WHEN LOWER(trafficSource.source) LIKE '%bing%' 
      THEN 'bing'
    WHEN LOWER(trafficSource.source) LIKE '%facebook%' OR LOWER(trafficSource.source) LIKE '%fb%'
      THEN 'facebook'
    WHEN LOWER(trafficSource.source) LIKE '%twitter%' 
      THEN 'twitter'
    WHEN LOWER(trafficSource.source) LIKE '%youtube%' 
      THEN 'youtube'
    WHEN LOWER(trafficSource.source) LIKE '%instagram%' 
      THEN 'instagram'
    WHEN LOWER(trafficSource.source) LIKE '%linkedin%' 
      THEN 'linkedin'
    ELSE trafficSource.source
  END AS traffic_source_grouped,

  -- ============================================================================
  -- Product Category (First level after "Home")
  -- ============================================================================
  -- Extracts top-level category from product category path
  -- Example: "Home/Apparel/Kid's/Kids-Youth/" -> "Apparel"
  REGEXP_EXTRACT(
    hits.product[SAFE_OFFSET(0)].v2ProductCategory,
    r'^Home/([^/]+)'
  ) AS product_category,
  
  -- Full product category path (for reference)
  hits.product[SAFE_OFFSET(0)].v2ProductCategory AS product_category_full,

  -- ============================================================================
  -- E-commerce Action Type (for conversion analysis)
  -- ============================================================================
  -- Action types: 0=unknown, 1=click, 2=detail view, 3=add to cart, 
  --               4=remove from cart, 5=checkout, 6=purchase, 7=refund
  hits.eCommerceAction.action_type AS ecommerce_action_type,
  
  -- Boolean flag for conversion (purchase)
  CASE 
    WHEN hits.eCommerceAction.action_type = '6' THEN TRUE
    WHEN hits.type = 'TRANSACTION' THEN TRUE
    ELSE FALSE
  END AS is_conversion

FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS hits

-- Filter only page and transaction types
WHERE hits.type IN ('PAGE', 'TRANSACTION')

-- Optional: Filter to specific date range
AND date BETWEEN '20170101' AND '20170108'

ORDER BY RAND()
LIMIT 10
;