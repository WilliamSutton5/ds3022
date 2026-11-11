import websocket
import json
import logging
import os
import signal
import sys
import threading
import time
from confluent_kafka.admin import AdminClient
from quixstreams import Application
from quixstreams.models import TopicConfig

"""
This script demonstrates how to use a Kafka producer to stream real-time
events from Bluesky Jetstream to a Kafka topic.

NOTE that it does not parse/consume messages, it publishes/produces them to Kafka.

If the connection drops, it reconnects with exponential backoff and resumes from the
last event it saw, using Jetstream's `cursor` parameter (a unix timestamp in microseconds).
The cursor is also saved to `jetstream_cursor.txt` (every REPORT_INTERVAL seconds and on exit)
and loaded on startup, so a restart replays the gap instead of skipping ahead. Logs go to
`bluesky-producer.log` as well as stdout.

(Already done from section below and moved up to here):
- Start the Kafka brokers: `docker compose up -d` from `06_kafka/`, and confirm they are up
    in the Redpanda Console.

Enable Kafka publishing:
- Uncomment the Kafka setup block below (`KAFKA_BROKER`, `app`, `topic`, `producer`).
    The imports it needs are already at the top of the file.
- In `on_message`, replace `print(did)` with a produce step: serialize the event with
    `topic.serialize()` (DID as the key, the full event as the value), then pass the
    serialized key and value to `producer.produce()` along with the topic name.
- Uncomment `producer.flush()` in the `finally` block so buffered messages are sent on exit.
- Run the script, then confirm messages are arriving in the `bluesky-events` topic
      (Redpanda Console, or run `bluesky-consumer.py`).
- OPTIONAL: Implement a method to measure/count msgs/sec every 5 seconds and print to stdout 
      as the script runs.
- My Note: Please present me with a plan and any choices/options I need to think about

      
Ways to make this more robust once Kafka publishing is enabled:
- (DONE) Save the cursor to a small local file (periodically and on exit) and load it on startup, so a
  restart resumes where it left off instead of skipping ahead. Ignore a saved cursor that is
  older than the topic's retention. TEST the cursor by running your script for a couple of minutes,
  then force it to stop using Ctrl+C, wait 5-10 seconds, then start it back up. You should see a
  large number of messages in the next count of msgs/sec.
- (DONE) Add a delivery callback (`on_delivery`) so you know which messages Kafka accepted or rejected.
- (DONE) Turn on `enable.idempotence` (implies `acks=all`) so the producer's internal retries can't
  create duplicates or reorder messages. Consider `compression.type` and larger batches too.
- (DONE) Set the Kafka message timestamp to the event time (`time_us`) instead of the time it was received.
- (DONE) Only advance the cursor after an event is safely handed to the producer, not when it arrives.
- (DONE) Catch `BufferError` from `produce()`: it means Kafka is slow or down and the local buffer is full.
- (DONE) When a message fails or the buffer fills, stop reading the stream, flush, and replay from the
  earliest failed event's cursor. Jetstream can act as your replay log.
- (DONE) Before reconnecting, check that Kafka is reachable, and exit with an error after a few failed tries.
- (DONE) Always `flush()` the producer before exiting, and handle SIGTERM (e.g. `docker stop`) as well as Ctrl-C.
- (DONE) Reconnect if the stream goes silent for too long, not just when the connection drops.
- (DONE) Rotate between Jetstream servers (jetstream1/2, us-east/us-west) on reconnect.
- (DONE) Expect duplicates (at-least-once delivery): replays and the reconnect rewind can resend events.
  How would a consumer detect and drop them?
    Answer: give each event a natural ID of did + commit.collection + commit.rkey + commit.rev
    (rev is unique per repo commit; collection + rkey separate the ops within one commit). Don't use
    time_us: each Jetstream server stamps its own, so the same event differs across servers. The
    consumer atomically records each ID in Redis (`SET id 1 NX EX <ttl>`) and drops the event if the
    ID was already there. See bluesky-consumer.py.
"""

# Jetstream Bluesky collections (posts, likes, reposts, follows, blocks, profiles, etc.)
JETSTREAM_PATH = "/subscribe?wantedCollections=app.bsky.*"
# Public Jetstream servers, rotated through on Jetstream-side problems (drop, error, silence)
JETSTREAM_SERVERS = [
    "jetstream2.us-west.bsky.network",
    "jetstream1.us-west.bsky.network",
    "jetstream1.us-east.bsky.network",
    "jetstream2.us-east.bsky.network",
]
CURSOR_REWIND_US = 2_000_000               # On reconnect, rewind 2s so no events are missed (may produce a few duplicates)
SWITCH_REWIND_US = 10_000_000              # Rewind 10s after changing servers: each server stamps its own time_us
SILENCE_TIMEOUT = 30                       # Reconnect if a connected stream sends nothing for this many seconds
MAX_BACKOFF = 60                           # Max seconds to wait between reconnect attempts
TOPIC_RETENTION_MS = 3 * 60 * 60 * 1000    # 3 hours
REPORT_INTERVAL = 5                        # Seconds between msgs/sec printouts (and cursor saves)
STALE_MARGIN_US = 10 * 60 * 1_000_000      # Treat a saved cursor as stale 10 min before it hits topic retention
KAFKA_CHECK_TRIES = 5                      # Reachability checks (backoff 1, 2, 4, 8, 16s) before exiting with an error
FLUSH_TIMEOUT = 35                         # Seconds to wait on flush; > message.timeout.ms so every message resolves
EXIT_FLUSH_TIMEOUT = 8                     # Shorter flush on exit: `docker stop` sends SIGKILL after 10s
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CURSOR_FILE = os.path.join(SCRIPT_DIR, "jetstream_cursor.txt")
LOG_FILE = os.path.join(SCRIPT_DIR, "bluesky-producer.log")

# Log to both a local file and stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
log = logging.getLogger("bluesky-producer")

cursor = None                              # time_us of the most recent event seen; used to resume after a disconnect
backoff = 1                                # Current reconnect delay in seconds
stop = False                               # Set on Ctrl-C (or a fatal producer error) to break out of the reconnect loop
exit_code = 0                              # Non-zero when exiting because of an error
replay_from = None                         # Earliest time_us that failed to reach Kafka; set = replay pending
ws = None                                  # Current WebSocketApp, so failure handlers can close it
connected = False                          # True while a websocket is open (the silence watchdog only acts then)
last_msg_at = time.monotonic()             # When the last event arrived, for the silence watchdog
server_idx = 0                             # Index into JETSTREAM_SERVERS of the current server
# Start with the larger rewind: a saved cursor may have come from a different server
switched = True
msg_count = 0                              # Messages produced since the last rate printout
delivered = 0                              # Messages Kafka acknowledged since the last printout
failed = 0                                 # Messages Kafka rejected/timed out since the last printout
last_report = time.time()                  # When the last rate printout happened

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")

app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={
        "broker.address.family": "v4",
        # Delivery guarantees: idempotence stops internal retries from duplicating/reordering messages
        "enable.idempotence": True,
        "acks": "all",                     # Implied by idempotence; set explicitly for clarity
        "message.timeout.ms": 30000,       # Report a failed delivery after 30s (default 5 min)
        # Throughput: bigger batches compress better and mean fewer requests
        "compression.type": "zstd",        # JSON compresses well; zstd gives the best ratio
        "linger.ms": 50,                   # Wait up to 50ms to fill a batch
        "batch.size": 1_000_000,           # Batch up to ~1MB (the broker's default max message size)
    }
)
# Admin client for Kafka reachability checks (a real metadata request, not just an open port)
admin = AdminClient({"bootstrap.servers": KAFKA_BROKER, "broker.address.family": "v4"})


def load_cursor():
    """Return the saved cursor, or None if it is missing, unreadable, or too close to the topic's retention."""
    try:
        with open(CURSOR_FILE) as f:
            saved = int(f.read().strip())
    except FileNotFoundError:
        log.info("No saved cursor; starting live")
        return None
    except (ValueError, OSError) as e:
        log.warning(f"Unreadable cursor file ({e}); starting live")
        return None

    # Events older than the topic's retention would be deleted by Kafka anyway, so don't replay them
    age_us = time.time() * 1_000_000 - saved
    if age_us > TOPIC_RETENTION_MS * 1000 - STALE_MARGIN_US:
        log.warning(f"Saved cursor is {age_us / 3.6e9:.1f}h old (past topic retention); starting live")
        return None
    log.info(f"Loaded saved cursor {saved} ({age_us / 1e6:.0f}s old)")
    return saved

def save_cursor():
    """Atomically write the cursor (temp file + rename) so a crash mid-write can't corrupt it."""
    if cursor is None:
        return
    try:
        tmp = CURSOR_FILE + ".tmp"
        with open(tmp, "w") as f:
            f.write(str(cursor))
        os.replace(tmp, CURSOR_FILE)
    except OSError as e:
        log.error(f"Failed to save cursor: {e}")

def kafka_reachable():
    """Return True if the brokers answer a metadata request."""
    try:
        return bool(admin.list_topics(timeout=5).brokers)
    except Exception as e:
        log.warning(f"Kafka check failed: {e}")
        return False

def wait_for_kafka():
    """Check Kafka up to KAFKA_CHECK_TRIES times with exponential backoff; return False if it never answers."""
    for i in range(KAFKA_CHECK_TRIES):
        if kafka_reachable():
            return True
        if i < KAFKA_CHECK_TRIES - 1:      # No point sleeping after the final try
            log.warning(f"Kafka unreachable (try {i + 1}/{KAFKA_CHECK_TRIES}); retrying in {2 ** i}s")
            time.sleep(2 ** i)
    return False

def mark_failed(time_us):
    """Record a failed event: pin the cursor to the earliest failure, save it, and stop reading the stream."""
    global replay_from, cursor
    replay_from = time_us if replay_from is None else min(replay_from, time_us)
    cursor = replay_from if cursor is None else min(cursor, replay_from)
    save_cursor()                          # So a crash before the replay still resumes from the failure
    if ws:
        ws.close()                         # run_forever returns; the reconnect loop flushes and replays

def on_delivery(err, msg, time_us):
    """Called (during produce/flush polling) once Kafka accepts or rejects a message; count both, handle failures."""
    global delivered, failed, stop, exit_code
    if err is None:
        delivered += 1
        return
    failed += 1
    # time_us is passed in because msg.timestamp() is unset on failed messages; it's the replay cursor
    log.error(f"Delivery failed for {msg.key()} (time_us {time_us}): {err}")
    # A fatal error means this producer can't send anything again: exit so a restart replays from the cursor
    if err.fatal():
        log.critical("Fatal producer error; exiting")
        stop, exit_code = True, 1
    mark_failed(time_us)

def watchdog():
    """Background thread: close the websocket if a connected stream goes silent, so the loop reconnects."""
    while True:
        time.sleep(5)
        silent = time.monotonic() - last_msg_at
        if connected and replay_from is None and silent > SILENCE_TIMEOUT:
            log.warning(f"Stream silent for {silent:.0f}s; reconnecting")
            ws.close()

def on_open(ws):
    """Connection established: reset the backoff delay and the silence timer."""
    global backoff, connected, last_msg_at
    backoff, connected, last_msg_at = 1, True, time.monotonic()
    log.info(f"Connected to {JETSTREAM_SERVERS[server_idx]}{f' (resuming from cursor {cursor})' if cursor else ''}")

def on_message(ws, message):
    """Handle each Jetstream event: produce commits to Kafka keyed by DID, then advance the cursor."""
    global cursor, msg_count, last_report, delivered, failed, last_msg_at
    last_msg_at = time.monotonic()         # Any event (even one ignored below) proves the stream is alive
    # A replay is pending (or we're stopping): ignore events still arriving before the socket closes
    if replay_from is not None or stop:
        return
    try:
        data = json.loads(message)

        # Jetstream also sends 'identity' and 'account' events (no 'commit' field); only commits are produced
        if data.get("kind") == "commit":
            # Serialize (DID key, full event value), then hand off to the producer's buffer.
            # The DID (decentralized identifier) uniquely identifies the acting user.
            msg = topic.serialize(key=data["did"], value=data)
            # Kafka timestamp = event time (ms), not receive time; on_delivery reports the outcome
            t = data["time_us"]
            try:
                # On a full buffer, wait 1s once (not quixstreams' default 10 x 30s, which would stall the websocket)
                producer.produce(
                    topic=topic.name, key=msg.key, value=msg.value, timestamp=t // 1000,
                    on_delivery=lambda err, m, t=t: on_delivery(err, m, t),
                    buffer_error_max_tries=2, poll_timeout=1,
                )
            except BufferError:
                log.error(f"Producer buffer full (Kafka slow or down) at time_us {t}")
                mark_failed(t)
                return
            msg_count += 1

        # Advance the resume point only once the event is handed to the producer (or deliberately skipped)
        cursor = data.get("time_us", cursor)

        # Every REPORT_INTERVAL seconds: print produced msgs / actual elapsed time, save the cursor, reset
        now = time.time()
        if now - last_report >= REPORT_INTERVAL:
            log.info(f"{msg_count / (now - last_report):.1f} msgs/sec | delivered {delivered}, failed {failed}")
            save_cursor()
            msg_count, delivered, failed, last_report = 0, 0, 0, now

    except (json.JSONDecodeError, KeyError) as e:
        log.warning(f"Bad message ({type(e).__name__}: {e})")
    except Exception as e:
        log.error(f"Unexpected error handling message: {e}")

def on_error(ws, error):
    """Report WebSocket-level errors. Ctrl-C/SIGTERM arrive here too (websocket-client swallows them), so flag a stop."""
    global stop
    if isinstance(error, KeyboardInterrupt):
        stop = True
    else:
        log.error(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    log.info(f"WebSocket closed ({close_status_code}: {close_msg})")

# Check Kafka before anything touches it: app.topic() below fetches/creates the topic on the broker
if not wait_for_kafka():
    log.error(f"Kafka unreachable after {KAFKA_CHECK_TRIES} tries; exiting")
    sys.exit(1)

topic = app.topic(
    name="bluesky-events",
    key_serializer="str",                  # DID string key; consumer decodes as utf8
    value_serializer="json",
    config=TopicConfig(
        num_partitions=1,
        replication_factor=3,
        extra_config={"retention.ms": str(TOPIC_RETENTION_MS)}
    )
)

# Create a kafka producer, reused for every message
producer = app.get_producer()

# Resume from the last saved position, if it's still within the topic's retention
cursor = load_cursor()

# Treat SIGTERM (e.g. `docker stop`, `kill`) exactly like Ctrl-C, so it takes the same flush-and-save path
signal.signal(signal.SIGTERM, signal.default_int_handler)
threading.Thread(target=watchdog, daemon=True).start()

# Reconnect loop: each pass checks Kafka, opens a connection (resuming from the cursor if we have one)
# and runs until it drops; then replay after a Kafka failure, or rotate servers with exponential backoff.
try:
    while not stop:
        if not wait_for_kafka():
            log.error(f"Kafka unreachable after {KAFKA_CHECK_TRIES} tries; exiting")
            exit_code = 1
            break
        rewind = SWITCH_REWIND_US if switched else CURSOR_REWIND_US
        url = f"wss://{JETSTREAM_SERVERS[server_idx]}{JETSTREAM_PATH}" + (f"&cursor={cursor - rewind}" if cursor else "")
        switched = False
        ws = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        # Pings detect silently dead connections; reconnect=0 hands reconnection to this loop
        ws.run_forever(ping_interval=30, ping_timeout=10, reconnect=0)
        connected = False
        if stop:
            break
        # Kafka failure: drain the buffer (more failures can only lower replay_from), then replay from
        # the earliest failed event. Skip the Jetstream backoff; wait_for_kafka handles the waiting.
        if replay_from is not None:
            log.warning("Kafka delivery failed; stopped stream, flushing before replay")
            producer.flush(FLUSH_TIMEOUT)
            cursor, replay_from = replay_from, None
            log.info(f"Replaying from cursor {cursor}")
            continue
        # Jetstream problem (drop, error, or silence): move to the next server
        server_idx = (server_idx + 1) % len(JETSTREAM_SERVERS)
        switched = True
        log.info(f"Disconnected. Reconnecting to {JETSTREAM_SERVERS[server_idx]} in {backoff}s...")
        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF)
except KeyboardInterrupt:
    pass
except Exception:
    log.exception("Unexpected error in main loop; exiting")
    exit_code = 1
finally:
    # Flush first so the saved cursor only covers events Kafka has accepted
    remaining = producer.flush(EXIT_FLUSH_TIMEOUT)
    if remaining:
        log.warning(f"{remaining} messages not flushed before exit; events after the saved cursor may be missing")
    save_cursor()
    log.info(f"Stopped. Saved cursor {cursor}")
sys.exit(exit_code)
