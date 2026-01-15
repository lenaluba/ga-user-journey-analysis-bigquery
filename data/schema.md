# Data Schema

Field definitions for the preprocessed GA data output.

## Session & Event Identifiers

| Field | Type | Description |
|-------|------|-------------|
| `session_id` | STRING | Unique session identifier (`fullVisitorId-visitId`) |
| `trace_time` | INTEGER | Event timestamp in milliseconds (visitStartTime * 1000 + hit.time) |
| `event_seq_num` | INTEGER | Sequential hit number within the session |

## URL Fields

| Field | Type | Description |
|-------|------|-------------|
| `url_path` | STRING | Page URL path (e.g., `/google+redesign/accessories`) |
| `url_host` | STRING | Hostname (e.g., `shop.googlemerchandisestore.com`) |
| `url` | STRING | Full URL |

## Device Information

| Field | Type | Description |
|-------|------|-------------|
| `device_type` | STRING | Device category: `desktop`, `mobile`, `tablet` |

## Page Classification

| Field | Type | Description |
|-------|------|-------------|
| `page_type` | STRING | Raw GA hit type: `PAGE`, `TRANSACTION` |
| `page_template` | STRING | Derived page classification (see values below) |

### Page Template Values

| Value | Description |
|-------|-------------|
| `Homepage` | Site homepage (`/`, `/home`, `/index.html`) |
| `ProductDetail` | Product detail pages (3+ URL segments) |
| `CategoryPage` | Category listing pages (2 URL segments) |
| `TopCategory` | Top-level category pages (1 URL segment) |
| `Cart` | Shopping cart/basket pages |
| `Checkout` | Checkout and payment pages |
| `OrderConfirmation` | Order confirmation/thank you pages |
| `SearchResults` | Search results pages |
| `Account` | Login, signup, account pages |
| `StoreLocator` | Physical store finder pages |
| `ProductQuickview` | Product quickview overlays |
| `Asset` | Static assets (js, css, images) |
| `Other` | Unclassified pages |

## Traffic Source Fields

| Field | Type | Description |
|-------|------|-------------|
| `traffic_source` | STRING | Raw traffic source value |
| `traffic_medium` | STRING | Traffic medium (`organic`, `cpc`, `referral`, `(none)`) |
| `traffic_campaign` | STRING | Campaign name |
| `traffic_ad_content` | STRING | Ad content identifier |
| `traffic_keyword` | STRING | Search keyword (often `(not provided)`) |
| `gclid` | STRING | Google Click ID for paid campaigns |
| `traffic_source_grouped` | STRING | Normalized source for easier grouping |

### Traffic Source Grouped Values

| Value | Includes |
|-------|----------|
| `google` | google, google.com |
| `facebook` | facebook, fb |
| `youtube` | youtube.com |
| `twitter` | twitter |
| `instagram` | instagram |
| `linkedin` | linkedin |
| `bing` | bing |
| `direct` | (direct), (not set), NULL |
| *other* | Original source value preserved |

## Product Information

| Field | Type | Description |
|-------|------|-------------|
| `product_category` | STRING | Top-level category (extracted from full path) |
| `product_category_full` | STRING | Full category path (e.g., `Home/Electronics/Audio/`) |

## E-commerce Fields

| Field | Type | Description |
|-------|------|-------------|
| `ecommerce_action_type` | STRING | Action type code (see values below) |
| `is_conversion` | BOOLEAN | TRUE if this event is a purchase |

### E-commerce Action Types

| Code | Action |
|------|--------|
| 0 | Unknown |
| 1 | Click |
| 2 | Product detail view |
| 3 | Add to cart |
| 4 | Remove from cart |
| 5 | Checkout |
| 6 | Purchase |
| 7 | Refund |
