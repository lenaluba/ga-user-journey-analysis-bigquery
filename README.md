# Google Analytics User Journey Analysis in BigQuery

A SQL-based customer journey analysis project using Google BigQuery, demonstrating how to transform raw Google Analytics data into actionable user journey insights.

## Overview

This project showcases techniques for analyzing e-commerce user journeys, including:
- Page template classification from URL patterns
- Session aggregation and journey path construction
- Traffic source analysis
- Conversion funnel metrics
- Device-based behavior comparison

## Data Source

This analysis uses the **Google Analytics Sample Dataset**, a public BigQuery dataset containing real, anonymized GA360 data from the [Google Merchandise Store](https://shop.googlemerchandisestore.com/).

**Dataset:** `bigquery-public-data.google_analytics_sample.ga_sessions_*`

> **Note:** This project was originally developed for a private e-commerce client. The methodology has been adapted here using public data for demonstration purposes.

## Repository Structure

```
ga-user-journey-analysis-bigquery/
├── README.md
├── sql/
│   ├── ga_data_preprocessing.sql    # Transform raw GA data
│   └── ga_user_journey.sql          # Journey analysis queries
└── data/
    ├── ga_preprocessed_sample.csv  # Sample output from preprocessing
    └── schema.md                    # Field descriptions
```

## SQL Scripts

### 1. Data Preprocessing (`sql/ga_data_preprocessing.sql`)

Transforms raw Google Analytics session data into a flattened, analysis-ready format.

**Key transformations:**
- Flattens nested `hits` array into individual page view records
- Creates `session_id` by combining `fullVisitorId` and `visitId`
- Classifies pages into templates based on URL patterns:
  - `Homepage`, `ProductDetail`, `CategoryPage`, `TopCategory`
  - `Cart`, `Checkout`, `OrderConfirmation`
  - `SearchResults`, `Account`, `StoreLocator`
- Extracts and normalizes traffic source information
- Flags conversion events (purchases)

### 2. User Journey Analysis (`sql/ga_user_journey.sql`)

Contains analysis queries built on the preprocessed data.

**Analysis sections:**
| Section | Description |
|---------|-------------|
| Q1: Journey Patterns | Path frequency, device breakdown, session duration |
| Q2: Traffic Source | Source distribution, bounce rates by source |
| Q3: Homepage Conversion | Product detail view rates from homepage entry |
| Q4: Category Impressions | Product views by category |
| Q5: Channel Analysis | Paid vs organic performance comparison |

## Output Schema

| Field | Description |
|-------|-------------|
| `session_id` | Unique session identifier |
| `trace_time` | Event timestamp (ms) |
| `event_seq_num` | Hit sequence within session |
| `url_path` | Page URL path |
| `device_type` | desktop, mobile, tablet |
| `page_template` | Classified page type |
| `traffic_source` | Raw traffic source |
| `traffic_source_grouped` | Normalized source (google, facebook, direct, etc.) |
| `traffic_medium` | Traffic medium (organic, cpc, referral, etc.) |
| `is_conversion` | Boolean purchase flag |

## How to Use

### Prerequisites
- Google Cloud account with BigQuery access
- Access to the public `bigquery-public-data` project

### Steps

1. **Run the preprocessing query**

   Open `sql/ga_data_preprocessing.sql` in BigQuery console and execute. Save results to a new table (e.g., `ga_preprocessed`):
   ```sql
   -- Modify date range as needed
   AND date BETWEEN '20170101' AND '20170801'
   ```

2. **Configure the analysis query**

   The analysis query uses template placeholders for the data source. In `sql/ga_user_journey.sql`, replace the placeholders with your BigQuery project and dataset:
   ```sql
   -- Replace these placeholders:
   FROM `{{PROJECT_ID}}.{{DATASET_ID}}.ga_preprocessed`

   -- With your actual values, e.g.:
   FROM `my-project.my_dataset.ga_preprocessed`
   ```

3. **Run analysis queries**

   Uncomment the desired analysis section in `ga_user_journey.sql` and execute.

## Sample Insights

The analysis can answer questions like:
- What are the most common user journey paths?
- How does bounce rate differ between traffic sources?
- What percentage of homepage visitors view a product detail page?
- How do conversion rates compare between paid and organic traffic?

## License

This project is available for educational and demonstration purposes.

## Acknowledgments

- Data source: [Google Analytics Sample Dataset](https://console.cloud.google.com/marketplace/product/obfuscated-ga360-data/obfuscated-ga360-data)
- Google Merchandise Store for the underlying e-commerce data