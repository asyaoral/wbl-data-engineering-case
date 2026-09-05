"""Producer entrypoint for fleet telemetry ingestion."""

import argparse
import json
import logging
import sys
import time
from typing import Optional

from confluent_kafka import Producer, KafkaException

from src.producer.simulator import TelemetrySimulator, TOPIC_RAW

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("fleet.producer")


def delivery_report(err, msg):
    """Callback triggered by confluent_kafka on delivery status."""
    if err is not None:
        logger.error("Message delivery failed: %s", err)
    else:
        logger.debug(
            "Message delivered to %s [%d] at offset %d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def create_producer(bootstrap_servers: str) -> Producer:
    """Create and return a configured Confluent Kafka Producer instance."""
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "fleet-telemetry-simulator",
        "acks": "all",
        "retries": 3,
        "linger.ms": 20,
    }
    return Producer(conf)


def run_producer(
    bootstrap_servers: str,
    topic: str,
    count: int = 0,
    interval: float = 0.2,
    dry_run: bool = False,
) -> None:
    """Publish simulated telemetry events to Kafka."""
    simulator = TelemetrySimulator()
    producer: Optional[Producer] = None

    if not dry_run:
        logger.info("Initializing Kafka producer connecting to %s ...", bootstrap_servers)
        producer = create_producer(bootstrap_servers)
    else:
        logger.info("Running in DRY-RUN mode (no Kafka messages dispatched).")

    sent_count = 0
    try:
        while True:
            event = simulator.generate_event()
            payload = json.dumps(event).encode("utf-8")
            key = event["vehicle_id"].encode("utf-8")

            if not dry_run and producer is not None:
                try:
                    producer.produce(
                        topic=topic,
                        key=key,
                        value=payload,
                        on_delivery=delivery_report,
                    )
                    producer.poll(0)
                except BufferError:
                    logger.warning("Local buffer full, flushing messages...")
                    producer.flush(5.0)
                    producer.produce(
                        topic=topic,
                        key=key,
                        value=payload,
                        on_delivery=delivery_report,
                    )
            else:
                logger.info("[DRY-RUN] %s: %s", key.decode(), json.dumps(event))

            sent_count += 1
            if sent_count % 10 == 0:
                logger.info("Published %d telemetry events to '%s'", sent_count, topic)

            if count > 0 and sent_count >= count:
                logger.info("Reached target limit of %d events. Stopping.", count)
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Shutdown requested by user.")
    finally:
        if producer is not None:
            logger.info("Flushing remaining messages before shutdown...")
            producer.flush(10.0)
            logger.info("Producer flush complete. Total events published: %d", sent_count)


def main():
    parser = argparse.ArgumentParser(description="Fleet Telemetry Kafka Producer")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        default=TOPIC_RAW,
        help=f"Kafka topic to publish to (default: {TOPIC_RAW})",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Number of events to generate (0 = continuous stream, default: 0)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.2,
        help="Delay between events in seconds (default: 0.2)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate without publishing to Kafka",
    )

    args = parser.parse_args()
    run_producer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        count=args.count,
        interval=args.interval,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
