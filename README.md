# wubba-lubba-dataflow

Rick says "Wubba Lubba Dub Dub" at random intervals (2-10 times per minute). GCP catches every one with Pub/Sub, Dataflow, BigQuery, and Looker Studio.

## Architecture

```
Cloud Scheduler (every 1 min)
  -> Cloud Function (publishes 2-10 random messages)
    -> Pub/Sub (wubba-lubba-topic)
      -> Dataflow / Apache Beam (fixed 1-min windows)
        -> BigQuery (wubba_metrics)
          -> Looker Studio (dashboard)
```

## Prerequisites

- GCP project with billing enabled
- `gcloud` CLI installed and authenticated
- Python 3.9+

## Quick Start

```bash
# Set your project
export GCP_PROJECT_ID="your-project-id"
export GCP_REGION="us-central1"  # optional, defaults to us-central1

# Install dependencies
pip install -r requirements.txt

# Create all GCP resources and deploy the Cloud Function
bash infra/setup.sh

# Deploy the Beam pipeline to Dataflow
python pipeline/pipeline.py \
  --project=$GCP_PROJECT_ID \
  --runner=DataflowRunner \
  --region=$GCP_REGION \
  --temp_location=gs://$GCP_PROJECT_ID-dataflow-temp/tmp \
  --staging_location=gs://$GCP_PROJECT_ID-dataflow-temp/staging \
  --requirements_file=requirements.txt \
  --max_num_workers=1 \
  --machine_type=n1-standard-1

# When done, drain the job and tear everything down
bash infra/teardown.sh
```

## Pausing and Resuming

Streaming Dataflow jobs bill continuously. When you step away, stop the pipeline and the scheduler:

```bash
# 1. Cancel the Dataflow job
gcloud dataflow jobs list --region=$GCP_REGION --status=active
gcloud dataflow jobs cancel <JOB_ID> --region=$GCP_REGION

# 2. Pause the scheduler so the Cloud Function stops firing
gcloud scheduler jobs pause wubba-lubba-trigger --location=$GCP_REGION
```

To pick it back up:

```bash
# 1. Resume the scheduler
gcloud scheduler jobs resume wubba-lubba-trigger --location=$GCP_REGION

# 2. Redeploy the pipeline
python pipeline/pipeline.py \
  --project=$GCP_PROJECT_ID \
  --runner=DataflowRunner \
  --region=$GCP_REGION \
  --temp_location=gs://$GCP_PROJECT_ID-dataflow-temp/tmp \
  --staging_location=gs://$GCP_PROJECT_ID-dataflow-temp/staging \
  --requirements_file=requirements.txt \
  --max_num_workers=1 \
  --machine_type=n1-standard-1
```

Any messages that queued in Pub/Sub while paused will be processed when the pipeline starts back up.

## Cost

Running in short bursts (30-60 min sessions), expect ~$3-4 for a weekend. The key rule: always drain or cancel the Dataflow job when you step away, since streaming jobs run until you stop them.

## Project Structure

```
wubba-lubba-dataflow/
├── infra/
│   ├── setup.sh          # creates all GCP resources
│   └── teardown.sh       # tears everything down
├── publisher/
│   └── main.py           # Cloud Function: 2-10 random messages per invocation
├── pipeline/
│   └── pipeline.py       # Apache Beam streaming pipeline
├── dashboard/
│   └── README.md         # Looker Studio setup instructions
└── requirements.txt
```
