from quixstreams import Application
import json
import logging
import os
import time
import redis

DEDUP_TTL = 30 * 60                        # Remember seen event IDs for 30 min (covers rewinds and Kafka-outage replays)
REPORT_INTERVAL = 5                        # Seconds between duplicate-count printouts
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bluesky-consumer.log")

# Log to both a local file and stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
log = logging.getLogger("bluesky-consumer")

def event_id(value):
    """Natural ID for a Jetstream commit event. rev is unique per repo commit; collection + rkey
    separate the ops within one commit. (time_us isn't used: each Jetstream server stamps its own.)"""
    c = value["commit"]
    return f"seen:{value['did']}:{c['collection']}:{c['rkey']}:{c['rev']}"

def is_duplicate(r, value):
    """Atomically record the event ID in Redis; True if it was already there.
    If Redis is down, fail open (process the event) rather than drop data."""
    try:
        return not r.set(event_id(value), 1, nx=True, ex=DEDUP_TTL)
    except redis.RedisError as e:
        log.warning(f"Redis dedup check failed ({e}); processing event anyway")
        return False

def main():
    app = Application(
        broker_address="127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
        loglevel="DEBUG",
        consumer_group="kafka-stream-counter",
        auto_offset_reset="earliest",
        producer_extra_config={
            # Resolve localhost to 127.0.0.1 to avoid IPv6 issues
            "broker.address.family": "v4",
        }
    )
    r = redis.Redis(host="localhost", port=6379, db=0)
    dupes, last_report = 0, time.time()    # Duplicates dropped since the last printout

    with app.get_consumer() as consumer:
        consumer.subscribe(["bluesky-events"])

        while True:
            msg = consumer.poll(1)

            if msg is None:
                print("Waiting...")
            elif msg.error() is not None:
                raise Exception(msg.error())
            else:
                key = msg.key().decode("utf8")
                value = json.loads(msg.value())
                offset = msg.offset()

                # Drop events already seen (producer replays and reconnect rewinds resend them)
                if is_duplicate(r, value):
                    dupes += 1
                else:
                    # get out various keys from the value
                    operation = value['commit']['operation']
                    collection = value['commit']['collection']

                    # Do something with the messages
                    # store incremental counts in redis by operation
                    # or collection

                consumer.store_offsets(msg)

            # Every REPORT_INTERVAL seconds, report how many duplicates were dropped
            if time.time() - last_report >= REPORT_INTERVAL:
                if dupes:
                    log.info(f"Dropped {dupes} duplicates")
                dupes, last_report = 0, time.time()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # redis_client = redis.Redis(host="localhost", port=6379, db=0)
        # for each key in redis, print the key and value
        # for key in redis_client.keys():
        #     print(f"Type: {key.decode('utf8')} | Count: {redis_client.get(key).decode('utf8')}")
        # redis_client.close()
        pass
