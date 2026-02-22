import logging
import os
import random
import signal
import threading
import time
from typing import Any, Dict, Optional

from PIL import Image, ImageDraw
from dotenv import load_dotenv
from paho.mqtt.client import Client

from logging_config import setup_logging
from system_info import SystemInfoProducer
from display_utils import page_manager, push_node_metrics, make_node_detail_page
from display_device import init_device, set_backlight, transition_backlight, cleanup_device
from node_mqtt import MultiNodeCollector, publish_node_metrics
from ha_mqtt import setup_ha_entities

# Load environment variables from .env file
load_dotenv()
setup_logging()

logger = logging.getLogger(__name__)

SIMULATOR_MODE        = os.getenv("SIMULATOR_MODE", "false").lower() == "true"
OFFLINE_REMOVE_HOURS  = float(os.getenv("OFFLINE_REMOVE_HOURS", "24"))
_ONLINE_THRESHOLD_SEC = 30.0
_REMOVE_THRESHOLD_SEC = OFFLINE_REMOVE_HOURS * 3600

# ============================================================================
# Initialization
# ============================================================================

# System metrics producer — collects every 1 second, fan-out to subscribers
sys_info_producer = SystemInfoProducer(update_interval=1.0)
sys_info_producer.start()

display_consumer = sys_info_producer.subscribe()
sensor_consumer  = sys_info_producer.subscribe()

# Display hardware (or simulator)
device, backlight = init_device(SIMULATOR_MODE)

# ============================================================================
# Multi-Node Setup
# ============================================================================

NODES = {
    "node1": "rack/node1",
    "node2": "rack/node2",
    "node3": "rack/node3",
}

node_collector = MultiNodeCollector(NODES)

# node1 is the local machine — always online, register its detail page at startup
_registered_node_pages: set = {"node1"}
page_manager.register("node1 detail", make_node_detail_page("node1"))

mqtt_host = os.getenv("MQTT_HOST")
if mqtt_host:
    node_collector.setup(
        mqtt_host,
        os.getenv("MQTT_USERNAME"),
        os.getenv("MQTT_PASSWORD"),
    )

# ============================================================================
# Home Assistant MQTT Setup
# ============================================================================

ha = setup_ha_entities(
    mqtt_host=mqtt_host or "",
    username=os.getenv("MQTT_USERNAME"),
    password=os.getenv("MQTT_PASSWORD"),
    set_backlight_fn=set_backlight,
    transition_backlight_fn=transition_backlight,
    page_next_fn=page_manager.next,
    page_prev_fn=page_manager.prev,
)

# Shared render-time measurement
_last_render_ms: Optional[float] = None
_render_ms_lock = threading.Lock()

# ============================================================================
# Simulator Mode — Mock Data Generator
# ============================================================================

def generate_mock_data() -> None:
    """Generate realistic mock system metrics for node2 and node3 in simulator mode."""
    node_data = {
        "node2": {"base_cpu": 25, "base_ram": 40, "base_temp": 52},
        "node3": {"base_cpu": 55, "base_ram": 65, "base_temp": 45},
    }
    for node_id, base_values in node_data.items():
        cpu_usage = base_values["base_cpu"] + random.uniform(-10, 15)
        ram_usage = base_values["base_ram"] + random.uniform(-5, 8)
        cpu_temp  = base_values["base_temp"] + random.uniform(-2, 3)
        cpu_freq  = random.uniform(1700, 1950)
        node_collector.latest_metrics[node_id] = {
            "cpu_usage":   max(0, min(100, cpu_usage)),
            "ram_usage":   max(0, min(100, ram_usage)),
            "cpu_temp":    cpu_temp,
            "cpu_freq":    cpu_freq,
            "disk_usage":  random.uniform(20, 75),
            "net_tx_rate": random.uniform(0, 500_000),
            "net_rx_rate": random.uniform(0, 2_000_000),
            "load_avg_1":  random.uniform(0.1, 2.5),
            "load_avg_5":  random.uniform(0.1, 2.0),
            "load_avg_15": random.uniform(0.1, 1.5),
        }

if SIMULATOR_MODE:
    logger.info("Simulator mode: Generating mock node data every second")

# ============================================================================
# Dynamic Page Management
# ============================================================================

def _sync_node_pages() -> None:
    """Add/remove per-node detail pages based on MQTT last-seen timestamps."""
    now = time.time()
    for node_id in NODES:
        if node_id == "node1":
            continue  # always registered
        last_seen = node_collector.get_last_seen(node_id)
        if last_seen is None:
            continue  # never seen; don't register yet
        page_name = f"{node_id} detail"
        if node_id not in _registered_node_pages:
            page_manager.register(page_name, make_node_detail_page(node_id))
            _registered_node_pages.add(node_id)
            logger.info("Registered detail page for %s", node_id)
        elif now - last_seen > _REMOVE_THRESHOLD_SEC:
            page_manager.unregister(page_name)
            _registered_node_pages.discard(node_id)
            logger.info("Removed detail page for %s (offline > %.0fh)", node_id, OFFLINE_REMOVE_HOURS)

# ============================================================================
# Main Loops
# ============================================================================

def display_loop() -> None:
    """Main display update loop — renders 3 nodes with metrics and sparklines."""
    try:
        while True:
            # Inject local system data into node1
            local_data = display_consumer.get_all()
            if (local_data.cpu_temp is not None or local_data.cpu_usage is not None
                    or local_data.ram_usage is not None or local_data.cpu_freq is not None):
                local_data.node_id = "node1"
                node_collector.latest_metrics["node1"] = {
                    "cpu_usage":   local_data.cpu_usage,
                    "ram_usage":   local_data.ram_usage,
                    "cpu_temp":    local_data.cpu_temp,
                    "cpu_freq":    local_data.cpu_freq,
                    "uptime":      local_data.uptime,
                    "disk_usage":  local_data.disk_usage,
                    "net_tx_rate": local_data.net_tx_rate,
                    "net_rx_rate": local_data.net_rx_rate,
                    "load_avg_1":  local_data.load_avg_1,
                    "load_avg_5":  local_data.load_avg_5,
                    "load_avg_15": local_data.load_avg_15,
                }

            if SIMULATOR_MODE:
                generate_mock_data()

            # Sync per-node detail pages with current availability
            _sync_node_pages()

            now = time.time()

            # Overview shows only online nodes (node1 always + online MQTT nodes), max 3
            online_mqtt = [
                nid for nid in NODES if nid != "node1"
                and (ls := node_collector.get_last_seen(nid)) is not None
                and now - ls <= _ONLINE_THRESHOLD_SEC
            ]
            overview_node_ids = (["node1"] + online_mqtt)[:3]

            # Build page data for all nodes
            page_data: Dict[str, Any] = {
                "node_ids":    overview_node_ids,
                "detail_node": "node1",  # fallback; factory pages override via closure
                "node_last_seen": {
                    nid: node_collector.get_last_seen(nid)
                    for nid in NODES if nid != "node1"
                },
            }
            for node_id in NODES:
                info = node_collector.get_node_info(node_id)
                page_data[node_id] = {
                    "cpu_usage":   info.cpu_usage,
                    "ram_usage":   info.ram_usage,
                    "cpu_temp":    info.cpu_temp,
                    "cpu_freq":    info.cpu_freq,
                    "uptime":      info.uptime,
                    "disk_usage":  info.disk_usage,
                    "net_tx_rate": info.net_tx_rate,
                    "net_rx_rate": info.net_rx_rate,
                    "load_avg_1":  info.load_avg_1,
                    "load_avg_5":  info.load_avg_5,
                    "load_avg_15": info.load_avg_15,
                }

            # Keep histogram buffers populated regardless of active page
            for node_id in NODES:
                nd = page_data.get(node_id, {})
                push_node_metrics(node_id, nd.get("cpu_usage"), nd.get("ram_usage"))

            frame_image = Image.new("RGB", (device.width, device.height), (0, 0, 0))
            frame_draw  = ImageDraw.Draw(frame_image)
            _t0 = time.monotonic()
            page_manager.render(frame_draw, device.width, device.height, page_data)
            device.display(frame_image)
            _render_ms = (time.monotonic() - _t0) * 1000

            with _render_ms_lock:
                global _last_render_ms
                _last_render_ms = _render_ms

            # Run faster during page switching so the switcher commits promptly
            time.sleep(0.2 if page_manager.is_switching else 1.0)
    except KeyboardInterrupt:
        logger.info("Display loop interrupted")
        raise


def sensor_loop() -> None:
    """Update HA sensors and publish node1 metrics to MQTT."""
    try:
        last_ha_update    = 0.0
        last_mqtt_publish = 0.0
        mqtt_client: Optional[Client] = None

        if mqtt_host:
            mqtt_client = Client()
            if os.getenv("MQTT_USERNAME") and os.getenv("MQTT_PASSWORD"):
                mqtt_client.username_pw_set(os.getenv("MQTT_USERNAME"), os.getenv("MQTT_PASSWORD"))
            try:
                mqtt_client.connect(mqtt_host, 1883, keepalive=60)
                mqtt_client.loop_start()
                logger.info("Connected MQTT client for node1 publishing")
            except Exception as e:
                logger.error("Failed to connect MQTT for publishing: %s", e)
                mqtt_client = None

        while True:
            now = time.time()

            # Update Home Assistant every ~10 seconds
            if now - last_ha_update >= 10.0:
                sys_info = sensor_consumer.get_all()

                if ha.sensor is not None and sys_info.cpu_temp is not None:
                    ha.sensor.set_state(sys_info.cpu_temp)
                    logger.debug("Updated sensor value: %.2f°C", sys_info.cpu_temp)

                with _render_ms_lock:
                    render_ms = _last_render_ms
                if ha.render_time_sensor is not None and render_ms is not None:
                    ha.render_time_sensor.set_state(round(render_ms, 1))
                    logger.debug("Screen render time: %.1fms", render_ms)

                last_ha_update = now

            # Publish node1 metrics to MQTT every 5 seconds
            if mqtt_client and now - last_mqtt_publish >= 5.0:
                sys_info = sensor_consumer.get_all()
                publish_node_metrics(mqtt_client, "rack/node1", sys_info)
                logger.debug("Published node1 metrics to MQTT")
                last_mqtt_publish = now

            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Sensor loop interrupted")
        if mqtt_client:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        raise


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    display_thread = threading.Thread(target=display_loop, daemon=True)
    sensor_thread  = threading.Thread(target=sensor_loop, daemon=True)

    display_thread.start()
    sensor_thread.start()

    try:
        if SIMULATOR_MODE:
            sim = device  # type: ignore[assignment]

            def _sigint_handler(_sig, _frame):  # type: ignore[no-untyped-def]
                if sim.root:
                    sim.root.quit()

            signal.signal(signal.SIGINT, _sigint_handler)

            sim.setup_window()
            assert sim.root is not None

            # Keyboard page switching in simulator: Left/Right arrow keys
            sim.root.bind("<Right>", lambda _e: page_manager.next())
            sim.root.bind("<Left>",  lambda _e: page_manager.prev())

            sim.root.mainloop()
        else:
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Exiting...")
        sys_info_producer.stop()
        cleanup_device()
        exit(0)
