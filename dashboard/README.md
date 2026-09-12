# Looker Studio Dashboard

## Setup

1. Go to [Looker Studio](https://lookerstudio.google.com/)
2. Create a new report
3. Add a BigQuery data source pointing to `wubba_lubba.wubba_metrics`
4. Build these charts:

### Time series: message count per window

- Chart type: Time series
- Dimension: `window_start`
- Metric: `message_count` (SUM)

### Scorecard: total Wubba Lubba Dub Dubs

- Chart type: Scorecard
- Metric: `message_count` (SUM across all rows)

### Bar chart: rate per window

- Chart type: Bar
- Dimension: `window_start`
- Metric: `avg_rate_per_sec` (AVG)
