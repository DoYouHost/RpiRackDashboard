"""MQTT multi-node metrics collection and publishing."""

import logging
import threading
import time
from typing import Dict, List, Optional

from paho.mqtt.client import Client, MQTTMessage

from system_info import SystemInfo

logger = logging.getLogger(__name__)


# Maps structured topic suffix → flat display key used by display_loop / get_node_info
_DISPLAY_KEY_MAP = {
    "cpu/usage":             "cpu_usage",
    "cpu/temp":              "cpu_temp",
    "cpu/freq":              "cpu_freq",
    "cpu/load_1":            "load_avg_1",
    "cpu/load_5":            "load_avg_5",
    "cpu/load_15":           "load_avg_15",
    "ram/percent":           "ram_usage",
    "disk/usage":            "disk_usage",
    "net/tx_rate":           "net_tx_rate",
    "net/rx_rate":           "net_rx_rate",
    "system/uptime":         "uptime",
    "system/throttled":      "throttle_state",
    "system/voltage/core":   "voltage_core",
    "system/voltage/sdram_c": "voltage_sdram_c",
    "system/voltage/sdram_i": "voltage_sdram_i",
    "system/voltage/sdram_p": "voltage_sdram_p",
}


class MultiNodeCollector:
    """Collects system metrics from multiple RPi nodes via MQTT."""

    def __init__(self, nodes: Dict[str, str], mqtt_client: Optional[Client] = None):
        """
        Initialize collector.

        Args:
            nodes: Dict of {"node_id": "mqtt_prefix"} e.g. {"node1": "rack/node1", "node2": "rack/node2"}
            mqtt_client: Optional MQTT client to use. If None, creates one.
        """
        self.nodes = nodes
        self.client = mqtt_client or Client()
        self.latest_metrics: Dict[str, dict] = {node: {} for node in nodes}
        self._last_seen: Dict[str, float] = {}
        self._lock = threading.Lock()
        self.running = False

    def setup(self, host: str, username: Optional[str] = None, password: Optional[str] = None) -> None:
        """Connect to MQTT broker and subscribe to node topics.

        Args:
            host: MQTT broker hostname
            username: Optional MQTT username
            password: Optional MQTT password
        """
        if username and password:
            self.client.username_pw_set(username, password)

        self.client.on_message = self._on_message
        self.client.connect(host, 1883, keepalive=60)

        for node_id, prefix in self.nodes.items():
            topic = f"{prefix}/#"
            self.client.subscribe(topic)
            logger.info("Subscribed to %s", topic)

        self.running = True
        self.client.loop_start()

    def _on_message(self, client, userdata, message: MQTTMessage) -> None:
        """Process incoming MQTT message."""
        try:
            # Format: rack / {node_id} / {suffix}   e.g. rack/node2/cpu/usage
            parts = message.topic.split("/", 2)
            if len(parts) < 3:
                return
            node_id = parts[1]
            suffix  = parts[2]

            try:
                value = float(message.payload.decode())
            except (ValueError, UnicodeDecodeError):
                return

            with self._lock:
                if node_id not in self.latest_metrics:
                    return
                self.latest_metrics[node_id][suffix] = value
                display_key = _DISPLAY_KEY_MAP.get(suffix)
                if display_key:
                    self.latest_metrics[node_id][display_key] = value
                self._last_seen[node_id] = time.time()
        except Exception as e:
            logger.error("Error processing MQTT message: %s", e)

    def get_last_seen(self, node_id: str) -> Optional[float]:
        """Return Unix timestamp of last received MQTT message, or None if never seen."""
        with self._lock:
            return self._last_seen.get(node_id)

    def get_online_nodes(self, timeout_sec: float = 30.0) -> List[str]:
        """Return node_ids that sent a message within the last timeout_sec seconds."""
        now = time.time()
        with self._lock:
            return [nid for nid, ts in self._last_seen.items() if now - ts <= timeout_sec]

    def get_node_info(self, node_id: str) -> SystemInfo:
        """Get latest metrics for a node as SystemInfo.

        Args:
            node_id: Node identifier

        Returns:
            SystemInfo with latest collected metrics
        """
        with self._lock:
            metrics = self.latest_metrics.get(node_id, {})
            return SystemInfo(
                node_id=node_id,
                cpu_temp=metrics.get("cpu_temp"),
                cpu_usage=metrics.get("cpu_usage"),
                ram_usage=metrics.get("ram_usage"),
                cpu_freq=metrics.get("cpu_freq"),
                uptime=metrics.get("uptime"),
                disk_usage=metrics.get("disk_usage"),
                net_tx_rate=metrics.get("net_tx_rate"),
                net_rx_rate=metrics.get("net_rx_rate"),
                load_avg_1=metrics.get("load_avg_1"),
                load_avg_5=metrics.get("load_avg_5"),
                load_avg_15=metrics.get("load_avg_15"),
            )

    def get_all_nodes(self) -> dict:
        """Get latest metrics for all nodes.

        Returns:
            Dict of {node_id: SystemInfo}
        """
        return {node_id: self.get_node_info(node_id) for node_id in self.nodes}

    def stop(self) -> None:
        """Stop collecting metrics."""
        self.running = False
        self.client.loop_stop()


def publish_node_metrics(mqtt_client: Client, prefix: str, sys_info: SystemInfo) -> None:
    """Publish all SystemInfo fields to MQTT under the given prefix.

    Args:
        mqtt_client: Connected paho MQTT client (loop must be running).
        prefix: Topic prefix, e.g. "rack/node1".
        sys_info: SystemInfo snapshot with metrics to publish.
    """
    def pub(topic: str, value) -> None:
        mqtt_client.publish(f"{prefix}/{topic}", str(value), qos=0)

    # CPU
    if sys_info.cpu_usage           is not None: pub("cpu/usage",          sys_info.cpu_usage)
    if sys_info.cpu_temp            is not None: pub("cpu/temp",           sys_info.cpu_temp)
    if sys_info.cpu_freq            is not None: pub("cpu/freq",           sys_info.cpu_freq)
    if sys_info.cpu_freq_min        is not None: pub("cpu/freq_min",       sys_info.cpu_freq_min)
    if sys_info.cpu_freq_max        is not None: pub("cpu/freq_max",       sys_info.cpu_freq_max)
    if sys_info.cpu_count           is not None: pub("cpu/count",          sys_info.cpu_count)
    if sys_info.cpu_count_physical  is not None: pub("cpu/count_physical", sys_info.cpu_count_physical)
    if sys_info.load_avg_1          is not None: pub("cpu/load_1",         sys_info.load_avg_1)
    if sys_info.load_avg_5          is not None: pub("cpu/load_5",         sys_info.load_avg_5)
    if sys_info.load_avg_15         is not None: pub("cpu/load_15",        sys_info.load_avg_15)
    if sys_info.cpu_ctx_switches    is not None: pub("cpu/ctx_switches",    sys_info.cpu_ctx_switches)
    if sys_info.cpu_interrupts      is not None: pub("cpu/interrupts",      sys_info.cpu_interrupts)
    if sys_info.cpu_soft_interrupts is not None: pub("cpu/soft_interrupts", sys_info.cpu_soft_interrupts)
    if sys_info.cpu_syscalls        is not None: pub("cpu/syscalls",        sys_info.cpu_syscalls)

    # RAM
    if sys_info.ram_usage     is not None: pub("ram/percent",   sys_info.ram_usage)
    if sys_info.ram_total     is not None: pub("ram/total",     sys_info.ram_total)
    if sys_info.ram_available is not None: pub("ram/available", sys_info.ram_available)
    if sys_info.ram_used      is not None: pub("ram/used",      sys_info.ram_used)
    if sys_info.ram_free      is not None: pub("ram/free",      sys_info.ram_free)
    for attr in ("active", "inactive", "buffers", "cached", "shared", "slab"):
        v = getattr(sys_info, f"ram_{attr}", None)
        if v is not None:
            pub(f"ram/{attr}", v)

    # Swap
    if sys_info.swap_percent is not None: pub("swap/percent", sys_info.swap_percent)
    if sys_info.swap_total   is not None: pub("swap/total",   sys_info.swap_total)
    if sys_info.swap_used    is not None: pub("swap/used",    sys_info.swap_used)
    if sys_info.swap_free    is not None: pub("swap/free",    sys_info.swap_free)
    if sys_info.swap_sin     is not None: pub("swap/sin",     sys_info.swap_sin)
    if sys_info.swap_sout    is not None: pub("swap/sout",    sys_info.swap_sout)

    # Disk
    if sys_info.disk_usage          is not None: pub("disk/usage",          sys_info.disk_usage)
    if sys_info.disk_io_read_bytes  is not None: pub("disk/io_read_bytes",  sys_info.disk_io_read_bytes)
    if sys_info.disk_io_write_bytes is not None: pub("disk/io_write_bytes", sys_info.disk_io_write_bytes)
    if sys_info.disk_io_read_count  is not None: pub("disk/io_read_count",  sys_info.disk_io_read_count)
    if sys_info.disk_io_write_count is not None: pub("disk/io_write_count", sys_info.disk_io_write_count)
    if sys_info.disk_io_read_time   is not None: pub("disk/io_read_time",   sys_info.disk_io_read_time)
    if sys_info.disk_io_write_time  is not None: pub("disk/io_write_time",  sys_info.disk_io_write_time)
    if sys_info.disk_partitions:
        for dev, d in sys_info.disk_partitions.items():
            for k, val in d.items():
                pub(f"disk/{dev}/{k}", val)

    # Net
    if sys_info.net_tx_rate  is not None: pub("net/tx_rate",  sys_info.net_tx_rate)
    if sys_info.net_rx_rate  is not None: pub("net/rx_rate",  sys_info.net_rx_rate)
    if sys_info.net_bytes_sent   is not None: pub("net/bytes_sent",   sys_info.net_bytes_sent)
    if sys_info.net_bytes_recv   is not None: pub("net/bytes_recv",   sys_info.net_bytes_recv)
    if sys_info.net_packets_sent is not None: pub("net/packets_sent", sys_info.net_packets_sent)
    if sys_info.net_packets_recv is not None: pub("net/packets_recv", sys_info.net_packets_recv)
    if sys_info.net_errin   is not None: pub("net/errin",   sys_info.net_errin)
    if sys_info.net_errout  is not None: pub("net/errout",  sys_info.net_errout)
    if sys_info.net_dropin  is not None: pub("net/dropin",  sys_info.net_dropin)
    if sys_info.net_dropout is not None: pub("net/dropout", sys_info.net_dropout)
    if sys_info.net_connections             is not None: pub("net/connections",             sys_info.net_connections)
    if sys_info.net_connections_established is not None: pub("net/connections_established", sys_info.net_connections_established)
    if sys_info.net_connections_listen      is not None: pub("net/connections_listen",      sys_info.net_connections_listen)
    if sys_info.net_if_stats:
        for iface, d in sys_info.net_if_stats.items():
            for k, val in d.items():
                pub(f"net/{iface}/{k}", val)

    # System
    if sys_info.uptime is not None: pub("system/uptime", sys_info.uptime)
    if sys_info.throttle_state is not None: pub("system/throttled", sys_info.throttle_state)

    # Voltages
    if sys_info.voltage_core is not None: pub("system/voltage/core", sys_info.voltage_core)
    if sys_info.voltage_sdram_c is not None: pub("system/voltage/sdram_c", sys_info.voltage_sdram_c)
    if sys_info.voltage_sdram_i is not None: pub("system/voltage/sdram_i", sys_info.voltage_sdram_i)
    if sys_info.voltage_sdram_p is not None: pub("system/voltage/sdram_p", sys_info.voltage_sdram_p)

    # Fans
    if sys_info.fans:
        for fan_key, speed in sys_info.fans.items():
            pub(f"fans/{fan_key}/speed", speed)
