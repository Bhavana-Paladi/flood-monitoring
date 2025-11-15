import csv
import json
import time
from pathlib import Path

from kafka import KafkaProducer


def get_producer():
    """
    Connect to local Kafka.
    """
    return KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def iter_readings_from_csv():
    """
    Read all readings from the CSV archive folder as Python dicts.
    """
    archive_dir = Path("data/archive")
    csv_files = sorted(archive_dir.glob("readings-full-*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in {archive_dir}. "
            f"Make sure you downloaded the readings-full-*.csv files."
        )

    for csv_path in csv_files:
        print(f"Streaming file: {csv_path}")
        with csv_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Each row is a dict: {"stationReference": "...", "dateTime": "...", "value": "...", ...}
                yield row


def main():
    topic = "flood_readings"
    producer = get_producer()

    msg_count = 0
    for row in iter_readings_from_csv():
        producer.send(topic, value=row)
        msg_count += 1

        # Flush periodically so messages are actually sent
        if msg_count % 1000 == 0:
            producer.flush()
            print(f"Sent {msg_count} messages so far...")

        # Slow down a bit so Spark streaming can keep up
        time.sleep(0.05)

    producer.flush()
    print(f"Done. Sent total {msg_count} messages to topic '{topic}'.")


if __name__ == "__main__":
    main()

