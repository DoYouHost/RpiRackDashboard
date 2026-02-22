"""node_agent.py — Standalone MQTT metric publisher for a single Rack RPi node.

Collects local system metrics and publishes them to MQTT under rack/<NODE_ID>/...
No display, no Home Assistant discovery, no multi-node collection.

Usage:
    python node_agent.py --node node1
    python node_agent.py --node node2 --interval 10
"""

import argparse
import logging
import os
import signal
import sys
import time

from dotenv import load_dotenv
from paho.mqtt.client import Client

from logging_config import setup_logging
from node_mqtt import publish_node_metrics
from system_info import SystemInfoProducer

logger = logging.getLogger(__name__)


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="RPi node metric publisher — sends system metrics to MQTT only.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python node_agent.py --node node1
  python node_agent.py --node node2 --interval 10

Metrics are published to: rack/<NODE_ID>/<metric>
MQTT connection settings are read from MQTT_HOST, MQTT_USERNAME, MQTT_PASSWORD
environment variables (or from a .env file in the current directory).
""",
    )
    parser.add_argument(
        "--node",
        required=True,
        metavar="NODE_ID",
        help="Node identifier (e.g. node1, node2). Metrics go to rack/<NODE_ID>/...",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        metavar="SECONDS",
        help="Publish interval in seconds (default: 5.0)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable DEBUG level logging (default: INFO)",
    )
    args = parser.parse_args()
    setup_logging(level=logging.DEBUG if args.debug else logging.INFO)

    mqtt_host = os.getenv("MQTT_HOST")
    if not mqtt_host:
        logger.error("MQTT_HOST environment variable is not set. Set it in .env or export it before running.")
        sys.exit(1)

    prefix = f"rack/{args.node}"
    logger.info("Node agent starting: node=%r, topic prefix=%r, interval=%ss", args.node, prefix, args.interval)

    # Start metrics producer (collects every 1 second in background)
    producer = SystemInfoProducer(update_interval=1.0)
    consumer = producer.subscribe()
    producer.start()

    # Connect MQTT client
    mqtt_client = Client()
    username = os.getenv("MQTT_USERNAME")
    password = os.getenv("MQTT_PASSWORD")
    if username and password:
        mqtt_client.username_pw_set(username, password)

    try:
        mqtt_client.connect(mqtt_host, 1883, keepalive=60)
        mqtt_client.loop_start()
        logger.info("Connected to MQTT broker at %s", mqtt_host)
    except Exception as e:
        logger.error("Could not connect to MQTT broker at %s: %s", mqtt_host, e)
        producer.stop()
        sys.exit(1)

    # Graceful shutdown on SIGINT (Ctrl+C) and SIGTERM
    running = True

    def _stop(*_) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    logger.info("Publishing metrics to %s/... — press Ctrl+C to stop", prefix)
    try:
        while running:
            sys_info = consumer.get_all()
            publish_node_metrics(mqtt_client, prefix, sys_info)
            logger.debug("Published metrics to %s/...", prefix)
            time.sleep(args.interval)
    finally:
        logger.info("Shutting down...")
        producer.stop()
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        logger.info("Done.")


if __name__ == "__main__":
    main()
