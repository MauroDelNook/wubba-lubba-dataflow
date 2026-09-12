import json
import os
import random
from datetime import datetime, timezone

import functions_framework
from google.cloud import pubsub_v1

publisher_client = pubsub_v1.PublisherClient()

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
TOPIC = os.environ.get("PUBSUB_TOPIC", "wubba-lubba-topic")


@functions_framework.http
def publish_wubba(request):
    topic_path = publisher_client.topic_path(PROJECT_ID, TOPIC)
    num_messages = random.randint(2, 10)

    futures = []
    for _ in range(num_messages):
        message = {
            "character": "Rick",
            "quote": "Wubba Lubba Dub Dub",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        data = json.dumps(message).encode("utf-8")
        future = publisher_client.publish(topic_path, data=data)
        futures.append(future)

    results = [f.result() for f in futures]

    return json.dumps({
        "published": num_messages,
        "message_ids": results,
    }), 200, {"Content-Type": "application/json"}
