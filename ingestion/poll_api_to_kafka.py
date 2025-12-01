import csv
import json
import os
import time
from kafka import KafkaProducer

ARCHIVE_DIR = "data/archive"
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "flood-readings"


def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        batch_size=32_768,
    )


def iter_archive_files():
    """
    Yield CSV file paths in data/archive in sorted order by filename.
    """
    files = [
        os.path.join(ARCHIVE_DIR, f)
        for f in os.listdir(ARCHIVE_DIR)
        if f.startswith("readings-full-") and f.endswith(".csv")
    ]
    return sorted(files, reverse=True)


def stream_file(producer, csv_path, sleep_between=0.01):
    """
    Read one archive CSV and send only water-level rows to Kafka.
    """
    print(f"Streaming file: {csv_path}")
    sent = 0

    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # We only care about water *level* measurements
            if row.get("parameter") != "level":
                continue

            try:
                level_value = float(row["value"])
            except (ValueError, TypeError):
                # Bad numeric value → skip
                continue

            event = {
                # Station identity
                "stationReference": row.get("stationReference"),
                "stationName": row.get("label"),
                # Time
                "timestamp": row.get("dateTime"),  # e.g. 2025-11-13T00:00:00Z
                "date": row.get("date"),
                # Measurement metadata
                "unitName": row.get("unitName"),       # m, mASD, mAOD
                "valueType": row.get("valueType"),     # instantaneous
                "datumType": row.get("datumType"),     # datumAOD / datumASD / ""
                # Actual water *level* in metres-ish
                "level": level_value,
            }

            producer.send(TOPIC, event)
            sent += 1

            if sent % 1000 == 0:
                print(f"  Sent {sent} messages so far...")

            time.sleep(sleep_between)

    print(f"Finished file {csv_path}, sent {sent} messages.")


def main():
    producer = get_producer()

    files = iter_archive_files()
    if not files:
        print("No archive files found in data/archive")
        return

    # Stream all archive files once (you can limit if you want)
    for path in files:
        stream_file(producer, path, sleep_between=0.005)

    producer.flush()
    producer.close()
    print("Done streaming all archive files.")


if __name__ == "__main__":
    main()
