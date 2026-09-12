import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.trigger import AfterWatermark
from apache_beam.transforms.window import TimestampedValue

logger = logging.getLogger(__name__)

TABLE_SCHEMA = {
    "fields": [
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "message_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "avg_rate_per_sec", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "min_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "max_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


class ParseMessage(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element.decode("utf-8"))
            ts = record["timestamp"]
            yield TimestampedValue(record, beam.utils.timestamp.Timestamp.from_rfc3339(ts))
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("Skipping malformed message: %s", e)


class AggregateWindow(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        timestamps = [msg["timestamp"] for msg in element]
        count = len(timestamps)
        window_start = window.start.to_utc_datetime().isoformat()
        window_end = window.end.to_utc_datetime().isoformat()
        window_duration_sec = (window.end - window.start).total_seconds()
        avg_rate = count / window_duration_sec if window_duration_sec > 0 else 0.0

        yield {
            "window_start": window_start,
            "window_end": window_end,
            "message_count": count,
            "avg_rate_per_sec": round(avg_rate, 4),
            "min_timestamp": min(timestamps),
            "max_timestamp": max(timestamps),
        }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--subscription", default="wubba-lubba-sub", help="Pub/Sub subscription name")
    parser.add_argument("--dataset", default="wubba_lubba", help="BigQuery dataset")
    parser.add_argument("--table", default="wubba_metrics", help="BigQuery table")
    parser.add_argument("--window-size", type=int, default=60, help="Fixed window size in seconds")

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    subscription_path = f"projects/{known_args.project}/subscriptions/{known_args.subscription}"
    table_ref = f"{known_args.project}:{known_args.dataset}.{known_args.table}"

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=subscription_path)
            | "ParseJSON" >> beam.ParDo(ParseMessage())
            | "FixedWindow" >> beam.WindowInto(
                FixedWindows(known_args.window_size),
                trigger=AfterWatermark(),
                accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING,
            )
            | "GroupAll" >> beam.CombineGlobally(beam.combiners.ToListCombineFn()).without_defaults()
            | "Aggregate" >> beam.ParDo(AggregateWindow())
            | "WriteBigQuery" >> WriteToBigQuery(
                table_ref,
                schema=TABLE_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
            )
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
