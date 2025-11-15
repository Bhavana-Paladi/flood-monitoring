#!/usr/bin/env python3
import csv
import glob
import json
import os
import time
from kafka import KafkaProducer

# Kafka config: you can override with env vars if needed
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flood-readings")


def get_producer() -> KafkaProducer:
    """
    Create a Kafka producer that sends JSON-encoded messages.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=50,
        batch_size=32 * 1024,
    )


def list_archive_files():
    """
    Return a sorted list of all readings-full-*.csv files in data/archive.
    """
    files = sorted(glob.glob("data/archive/readings-full-*.csv"))
    if not files:
        raise RuntimeError(
            "No files found matching data/archive/readings-full-*.csv. "
            "Check that you downloaded the archive CSVs correctly."
        )
    return files


def clean_row(row: dict):
    """
    Take a raw CSV row and:
    - drop rows with missing or malformed value
    - skip rows where value has '|' like '3.488|3.486'
    - return a JSON-serialisable dict with only the fields the streaming job needs
    """
    raw_val = row.get("value")

    # Drop empty, NaN or weird pipe-separated values
    if raw_val in (None, "", "NaN"):
        return None
    if "|" in raw_val:
        return None

    try:
        value = float(raw_val)
    except ValueError:
        return None

    # Only keep the columns the Spark stream_alerts.py expects
    payload = {
        "stationReference": row.get("stationReference"),
        "dateTime": row.get("dateTime"),
        "value": value,
        "parameter": row.get("parameter"),
        "qualifier": row.get("qualifier"),
        "riverName": row.get("riverName"),
        "catchmentName": row.get("catchmentName"),
        "town": row.get("town"),
    }

    # Require at least stationReference + dateTime
    if not payload["stationReference"] or not payload["dateTime"]:
        return None

    return payload


def main():
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}, topic={KAFKA_TOPIC}")
    producer = get_producer()

    files = list_archive_files()
    print("Files to stream (in order):")
    for f in files:
        print(f"  - {f}")

    total_sent = 0

    for path in files:
        print(f"\n=== Streaming file: {path} ===")
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                payload = clean_row(row)
                if payload is None:
                    continue

                producer.send(KAFKA_TOPIC, value=payload)
                total_sent += 1

                if total_sent % 1000 == 0:
                    print(f"Sent {total_sent} messages so far...")

                # Small delay so it behaves like a stream, not a crazy burst
                time.sleep(0.001)

        # Flush after each file so everything gets sent
        producer.flush()
        print(f"Finished file {path}. Total sent so far: {total_sent}")

    producer.flush()
    producer.close()
    print(f"\n✅ Done. Total messages sent: {total_sent}")


if __name__ == "__main__":
    main()
