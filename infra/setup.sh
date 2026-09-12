#!/usr/bin/env bash
set -euo pipefail

# ── Configuration ──────────────────────────────────────────────
PROJECT_ID="${GCP_PROJECT_ID:?Set GCP_PROJECT_ID before running this script}"
REGION="${GCP_REGION:-us-central1}"

TOPIC="wubba-lubba-topic"
SUBSCRIPTION="wubba-lubba-sub"
DATASET="wubba_lubba"
TABLE="wubba_metrics"
SCHEDULER_JOB="wubba-lubba-trigger"
FUNCTION_NAME="wubba-lubba-publisher"

echo "==> Using project: $PROJECT_ID, region: $REGION"
gcloud config set project "$PROJECT_ID"

# ── Enable APIs ────────────────────────────────────────────────
echo "==> Enabling APIs..."
gcloud services enable \
  pubsub.googleapis.com \
  dataflow.googleapis.com \
  bigquery.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudscheduler.googleapis.com \
  cloudbuild.googleapis.com

# ── Pub/Sub ────────────────────────────────────────────────────
echo "==> Creating Pub/Sub topic and subscription..."
gcloud pubsub topics create "$TOPIC" --quiet 2>/dev/null || echo "    Topic already exists"
gcloud pubsub subscriptions create "$SUBSCRIPTION" \
  --topic="$TOPIC" \
  --ack-deadline=60 \
  --quiet 2>/dev/null || echo "    Subscription already exists"

# ── BigQuery ───────────────────────────────────────────────────
echo "==> Creating BigQuery dataset and table..."
bq --location="$REGION" mk --dataset "$PROJECT_ID:$DATASET" 2>/dev/null || echo "    Dataset already exists"

bq mk --table "$PROJECT_ID:$DATASET.$TABLE" \
  window_start:TIMESTAMP,window_end:TIMESTAMP,message_count:INTEGER,avg_rate_per_sec:FLOAT,min_timestamp:TIMESTAMP,max_timestamp:TIMESTAMP \
  2>/dev/null || echo "    Table already exists"

# ── Cloud Function ─────────────────────────────────────────────
echo "==> Deploying Cloud Function..."
gcloud functions deploy "$FUNCTION_NAME" \
  --gen2 \
  --region="$REGION" \
  --runtime=python312 \
  --source=../publisher \
  --entry-point=publish_wubba \
  --trigger-http \
  --allow-unauthenticated \
  --memory=256MB \
  --timeout=60s \
  --quiet

FUNCTION_URL=$(gcloud functions describe "$FUNCTION_NAME" \
  --gen2 \
  --region="$REGION" \
  --format="value(serviceConfig.uri)")

echo "    Function URL: $FUNCTION_URL"

# ── Cloud Scheduler ───────────────────────────────────────────
echo "==> Creating Cloud Scheduler job..."
gcloud scheduler jobs create http "$SCHEDULER_JOB" \
  --location="$REGION" \
  --schedule="* * * * *" \
  --uri="$FUNCTION_URL" \
  --http-method=POST \
  --quiet 2>/dev/null || echo "    Scheduler job already exists"

echo ""
echo "==> Setup complete."
echo "    Topic:        $TOPIC"
echo "    Subscription: $SUBSCRIPTION"
echo "    BQ table:     $DATASET.$TABLE"
echo "    Scheduler:    $SCHEDULER_JOB (every 1 min)"
echo ""
echo "Next steps:"
echo "  1. Deploy the Beam pipeline (see pipeline/pipeline.py)"
echo "  2. Monitor in the Dataflow console"
echo "  3. Run infra/teardown.sh when done"
