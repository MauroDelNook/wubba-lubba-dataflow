# How Dataflow and Beam Work

A companion guide to the wubba-lubba-dataflow pipeline. This explains the core concepts behind Apache Beam and Google Cloud Dataflow, grounded in examples from this project.

## Batch vs Streaming

Data pipelines come in two flavors:

- **Batch**: process a finite dataset from start to finish. Think "read a CSV, transform it, write the output." The pipeline knows when the data ends.
- **Streaming**: process an infinite, continuous flow of data. Messages keep arriving and the pipeline never finishes on its own. Our pipeline is streaming because Pub/Sub messages arrive indefinitely, one batch per minute from the Cloud Function.

In Beam, the distinction is controlled by a single flag:

```python
pipeline_options.view_as(StandardOptions).streaming = True
```

The same pipeline code can often run in both modes. Beam calls a finite dataset a **bounded PCollection** and an infinite stream an **unbounded PCollection**.

## What is Apache Beam?

Beam is a unified programming model for defining data processing pipelines. You write the logic once, and a **runner** executes it on whatever engine you choose: Dataflow, Spark, Flink, or even locally with `DirectRunner`.

A Beam pipeline is a directed acyclic graph (DAG) of transforms:

```
Read -> Transform A -> Transform B -> Write
```

In our case:

```
ReadPubSub -> ParseJSON -> FixedWindow -> GroupAll -> Aggregate -> WriteBigQuery
```

Each step takes a PCollection as input and produces a PCollection as output.

## What is Dataflow?

Dataflow is Google Cloud's managed service for running Beam pipelines. When you submit a pipeline with `--runner=DataflowRunner`, Dataflow:

1. Packages your code and uploads it to GCS (the staging bucket)
2. Spins up worker VMs to execute the pipeline
3. Distributes work across workers automatically
4. Handles checkpointing, retries, and scaling
5. Keeps running until you cancel or drain the job (for streaming)

You don't manage servers, clusters, or scheduling. You pay for the VMs while they run, which is why draining the job when you're done matters.

## PCollections

A **PCollection** is Beam's abstraction for a dataset. It's an unordered bag of elements that flows through the pipeline. Unlike a Python list, you can't index into it or iterate over it directly. You can only apply transforms to it.

Each element in a PCollection has an associated **timestamp**. For streaming pipelines, this timestamp is critical because it determines which window an element falls into.

In our pipeline, we explicitly assign timestamps in `ParseMessage`:

```python
yield TimestampedValue(record, Timestamp.from_rfc3339(rfc3339_ts))
```

Without this, Beam would use the Pub/Sub publish time, which is close but not identical to the timestamp in our message payload.

## Transforms and DoFns

A **transform** is an operation on a PCollection. Beam provides built-in transforms (`Map`, `Filter`, `GroupByKey`, `CombineGlobally`) and lets you write custom ones using **DoFn** (pronounced "do function").

A DoFn is a class with a `process()` method that receives one element and yields zero or more output elements:

```python
class ParseMessage(beam.DoFn):
    def process(self, element):
        record = json.loads(element.decode("utf-8"))
        yield TimestampedValue(record, ...)
```

`yield` makes it a generator. You can yield multiple elements (fan-out), one element (1:1 mapping), or nothing (filtering). You apply a DoFn with `beam.ParDo()`:

```python
| "ParseJSON" >> beam.ParDo(ParseMessage())
```

## Windowing

Streaming data is infinite, so you can't just "process all of it." Windowing divides the unbounded stream into finite chunks based on time, so you can aggregate within each chunk.

### Fixed Windows

The simplest strategy. The timeline is divided into non-overlapping intervals of equal size. Every element falls into exactly one window based on its timestamp.

```python
beam.WindowInto(FixedWindows(60))  # 60-second windows
```

With 60-second windows, all messages timestamped between 16:23:00 and 16:23:59.999 go into one window, 16:24:00 to 16:24:59.999 into the next, and so on.

Our pipeline uses 1-minute fixed windows because the Cloud Scheduler fires every minute. Each window captures roughly one batch of messages.

### Other Window Types

Beam also supports:

- **Sliding windows**: overlapping intervals (e.g., 5-minute windows every 1 minute). An element can appear in multiple windows.
- **Session windows**: dynamic windows that group elements by activity. A new window opens when a message arrives after a gap of inactivity. Useful for user session analysis.
- **Global window**: one window for everything. The default, and what you get if you don't call `WindowInto`. Fine for batch, problematic for streaming because "everything" never ends.

## Watermarks

How does Beam know when a window is "done" and can be processed? It can't just wait for the clock to pass the window boundary, because messages can arrive late (network delays, retries, out-of-order delivery).

The **watermark** is Beam's estimate of how far along the stream has progressed. It's a timestamp that says: "I believe all data with event times up to this point has arrived." When the watermark passes the end of a window, that window is considered complete.

Dataflow tracks the watermark automatically based on the data source. For Pub/Sub, it uses the oldest unacknowledged message's publish time. If a message gets stuck or delayed, the watermark holds back, delaying window completion until the message is processed or expires.

You don't control the watermark directly, but you can influence how the pipeline reacts to it through **triggers**.

## Triggers and Accumulation

A **trigger** determines when a window's results are emitted. The default is `AfterWatermark()`, which fires once when the watermark passes the window's end:

```python
beam.WindowInto(
    FixedWindows(60),
    trigger=AfterWatermark(),
    accumulation_mode=AccumulationMode.DISCARDING,
)
```

Other trigger strategies exist:

- **AfterProcessingTime**: fire after a certain amount of wall-clock time
- **AfterCount**: fire after N elements arrive
- **Repeatedly**: re-fire a trigger periodically
- **AfterWatermark with early/late firings**: emit speculative early results before the window closes, then late corrections after

**Accumulation mode** controls what happens when a trigger fires multiple times for the same window:

- `DISCARDING`: each firing only includes elements since the last firing. Previous results are thrown away.
- `ACCUMULATING`: each firing includes all elements in the window so far. Results grow over time.

We use `DISCARDING` because we fire once per window and write to BigQuery. Accumulating would produce duplicate counts if a late firing ever happened.

## The Pipeline Step by Step

Here's what happens to a single batch of messages:

### 1. ReadPubSub

```python
beam.io.ReadFromPubSub(subscription=subscription_path)
```

Beam pulls messages from the Pub/Sub subscription. Each message arrives as raw bytes. Dataflow manages acknowledgment: if processing succeeds, the message is acked. If the worker crashes, unacked messages are redelivered.

### 2. ParseJSON

```python
beam.ParDo(ParseMessage())
```

Each raw byte string is decoded to JSON, and the record is paired with its timestamp using `TimestampedValue`. This tells Beam "this element's event time is X," which determines its window assignment.

The timestamp must be in RFC 3339 format for `Timestamp.from_rfc3339()`. Python's `isoformat()` produces `+00:00` for UTC, but Beam expects `Z`. That's why we do:

```python
rfc3339_ts = ts.replace("+00:00", "Z")
```

### 3. FixedWindow

```python
beam.WindowInto(FixedWindows(60), ...)
```

Each element is assigned to a 60-second window based on its timestamp. This is purely metadata at this point. The element doesn't move anywhere yet.

### 4. GroupAll

```python
beam.CombineGlobally(beam.combiners.ToListCombineFn()).without_defaults()
```

All elements within each window are collected into a single list. `CombineGlobally` works per-window in a windowed pipeline. The `.without_defaults()` is important: without it, Beam would emit an empty list for windows with no data.

After this step, each window produces one element: a list of all message records in that window.

### 5. Aggregate

```python
beam.ParDo(AggregateWindow())
```

The list is turned into a summary row: message count, average rate per second, min/max timestamps. The `window` parameter gives access to the window boundaries:

```python
def process(self, element, window=beam.DoFn.WindowParam):
    window_start_dt = window.start.to_utc_datetime()
    window_end_dt = window.end.to_utc_datetime()
```

`beam.DoFn.WindowParam` is a special parameter that Beam injects automatically. It gives you the window's start and end as Beam `Timestamp` objects, which you convert to Python `datetime` for arithmetic.

### 6. WriteBigQuery

```python
WriteToBigQuery(table_ref, schema=TABLE_SCHEMA, ...)
```

The summary dict is written to BigQuery. `WRITE_APPEND` adds rows without removing existing data. `CREATE_NEVER` means the table must already exist (created by `setup.sh`).

## DoFn Serialization (The Pickling Gotcha)

When you run with `DataflowRunner`, Beam serializes (pickles) your DoFn classes and sends them to remote worker VMs. Those workers are separate Python processes on separate machines. They don't share memory or module state with the machine that launched the pipeline.

This means module-level imports and variables from your main script are **not available** inside `process()` on the workers. The class is pickled, shipped, unpickled, and executed in a bare environment where only the standard library and your `requirements.txt` packages exist.

The fix is to import everything you need inside the `process()` method:

```python
class ParseMessage(beam.DoFn):
    def process(self, element):
        import json as _json
        from apache_beam.transforms.window import TimestampedValue
        from apache_beam.utils.timestamp import Timestamp
        # now these are available on the worker
```

This looks odd compared to normal Python style, but it's the standard pattern for Beam DoFns running on distributed runners. The `DirectRunner` (local testing) doesn't have this problem because everything runs in the same process, which makes it a sneaky bug: code works locally but fails on Dataflow.

## Beam Duration vs Python timedelta

Beam has its own `Duration` type for representing time differences. When you subtract two Beam `Timestamp` objects, you get a `Duration`, not a Python `timedelta`:

```python
# This fails:
duration = window.end - window.start  # Returns Beam Duration
duration.total_seconds()               # AttributeError: no such method

# This works:
start_dt = window.start.to_utc_datetime()  # Python datetime
end_dt = window.end.to_utc_datetime()      # Python datetime
duration = end_dt - start_dt               # Python timedelta
duration.total_seconds()                   # Works fine
```

Convert to Python `datetime` first, then subtract. The resulting `timedelta` has all the methods you'd expect.

## Pub/Sub and Dataflow Integration

Pub/Sub is the glue between the Cloud Function and the pipeline. Here's how the pieces connect:

- **Topic** (`wubba-lubba-topic`): the channel where the Cloud Function publishes messages. A topic can have multiple subscriptions.
- **Subscription** (`wubba-lubba-sub`): a named cursor on the topic. Dataflow reads from the subscription, not the topic directly. Each subscription gets its own copy of every message.
- **Ack deadline** (60 seconds): if Dataflow doesn't acknowledge a message within this time, Pub/Sub redelivers it. The pipeline must process each message faster than this deadline.
- **At-least-once delivery**: Pub/Sub guarantees every message is delivered at least once, but duplicates are possible. For this learning project, occasional duplicate counts are acceptable.

When no Dataflow job is running, messages accumulate in the subscription (retained for 7 days by default). When the pipeline starts back up, it drains the backlog before processing new messages.

## Runners

The runner is the execution engine. Beam abstracts the pipeline logic from the execution:

- **DirectRunner**: runs locally in a single Python process. Great for development and debugging. No GCP resources needed.
- **DataflowRunner**: runs on Google Cloud Dataflow. Handles scaling, fault tolerance, and monitoring. Costs money.
- **FlinkRunner, SparkRunner**: run on Apache Flink or Spark clusters. Same Beam code, different infrastructure.

To test locally before deploying:

```bash
python pipeline/pipeline.py \
  --project=wubba-lubba-dataflow \
  --runner=DirectRunner
```

This won't read from Pub/Sub (you'd need to mock the input), but it validates that the pipeline graph is well-formed and the transforms compile.

## Key Takeaways

1. **Streaming pipelines run forever.** You pay for compute the entire time. Always drain or cancel when you're done.
2. **Windowing converts infinite streams into finite chunks.** Without windows, you can't aggregate streaming data.
3. **Watermarks track progress.** They tell the system when it's safe to consider a window complete.
4. **DoFns get serialized.** Anything your `process()` method needs must be importable on the worker, not just on your laptop.
5. **Beam has its own types.** `Timestamp`, `Duration`, and `TimestampedValue` are not Python builtins. Know when to convert between Beam and Python types.
6. **Test locally first.** `DirectRunner` catches most pipeline construction errors without costing anything. But it won't catch serialization issues, so be aware of that gap.
