"""Consumer CLI entrypoint for raw fleet telemetry ingestion."""

import argparse
import logging
from pathlib import Path

from src.consumer.raw_consumer import RawTelemetryConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("fleet.consumer")


def main():
    parser = argparse.ArgumentParser(description="Fleet Telemetry Raw Kafka Consumer")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        default="fleet.telemetry.raw",
        help="Kafka topic to consume (default: fleet.telemetry.raw)",
    )
    parser.add_argument(
        "--group-id",
        default="fleet-raw-consumer-group",
        help="Consumer group ID (default: fleet-raw-consumer-group)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw"),
        help="Target folder for raw JSON lines files (default: data/raw)",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after consuming N messages (0 = continuous, default: 0)",
    )

    args = parser.parse_args()

    consumer = RawTelemetryConsumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        raw_dir=args.output_dir,
    )
    consumer.run(max_messages=args.max_messages)


if __name__ == "__main__":
    main()
