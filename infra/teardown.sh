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

# ── Drain active Dataflow jobs ────────────────────────────────
echo "==> Draining active Dataflow jobs..."
ACTIVE_JOBS=$(gcloud dataflow jobs list \
  --region="$REGION" \
  --status=active \
  --format="value(JOB_ID)" 2>/dev/null || true)

if [ -n "$ACTIVE_JOBS" ]; then
  for JOB_ID in $ACTIVE_JOBS; do
    echo "    Draining job: $JOB_ID"
    gcloud dataflow jobs drain "$JOB_ID" --region="$REGION" --quiet
  done
  echo "    Waiting for jobs to finish draining (this can take a few minutes)..."
  for JOB_ID in $ACTIVE_JOBS; do
    gcloud dataflow jobs show "$JOB_ID" --region="$REGION" --format="value(currentState)" 2>/dev/null || true
  done
else
  echo "    No active Dataflow jobs found"
fi

# ── Cloud Scheduler ──────────────────────────────────────────
echo "==> Deleting Cloud Scheduler job..."
gcloud scheduler jobs delete "$SCHEDULER_JOB" \
  --location="$REGION" \
  --quiet 2>/dev/null || echo "    Scheduler job not found"

# ── Cloud Function ───────────────────────────────────────────
echo "==> Deleting Cloud Function..."
gcloud functions delete "$FUNCTION_NAME" \
  --gen2 \
  --region="$REGION" \
  --quiet 2>/dev/null || echo "    Function not found"

# ── Pub/Sub ──────────────────────────────────────────────────
echo "==> Deleting Pub/Sub subscription and topic..."
gcloud pubsub subscriptions delete "$SUBSCRIPTION" \
  --quiet 2>/dev/null || echo "    Subscription not found"
gcloud pubsub topics delete "$TOPIC" \
  --quiet 2>/dev/null || echo "    Topic not found"

# ── BigQuery ─────────────────────────────────────────────────
echo "==> Deleting BigQuery table and dataset..."
bq rm -f -t "$PROJECT_ID:$DATASET.$TABLE" 2>/dev/null || echo "    Table not found"
bq rm -f -d "$PROJECT_ID:$DATASET" 2>/dev/null || echo "    Dataset not found"

echo ""
echo "==> Teardown complete."
echo "    Double-check the GCP console to confirm nothing is still running."
echo "    https://console.cloud.google.com/dataflow/jobs?project=$PROJECT_ID"
