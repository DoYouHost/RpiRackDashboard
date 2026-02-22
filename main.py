import time
import os
import threading
import json
from typing import Any, Dict, Optional
from PIL import Image, ImageDraw
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

SIMULATOR_MODE = os.getenv("SIMULATOR_MODE", "false").lower() == "true"

from ha_mqtt_discoverable import Settings, DeviceInfo
from ha_mqtt_discoverable.sensors import (
    Sensor,
    SensorInfo,
    Light,
    LightInfo,
    Button,
    ButtonInfo,
)
from paho.mqtt.client import Client, MQTTMessage
from system_info import SystemInfo, SystemInfoProducer
from display_utils import page_manager, push_node_metrics

# Initialize system info producer - runs every 1 second and collects data
sys_info_producer = SystemInfoProducer(update_interval=1.0)
sys_info_producer.start()

# Create independent consumer handles for display and sensor loops
display_consumer = sys_info_producer.subscribe()
sensor_consumer = sys_info_producer.subscribe()

if not SIMULATOR_MODE:
    try:
        import digitalio
        import board
        from luma.lcd.device import st7789
        from luma.core.interface.serial import spi
        import RPi.GPIO as GPIO
    except ImportError as e:
        print(f"FATAL: Hardware libraries not available: {e}")
        print("SIMULATOR_MODE=false was set but hardware is not available!")
        print("Either:")
        print("  1. Install missing libraries (pip install -r requirements.txt)")
        print("  2. Set SIMULATOR_MODE=true if running in simulator")
        exit(1)

if SIMULATOR_MODE:
    import tkinter as tk
    from PIL import ImageTk

# ============================================================================
# Display Device Classes
# ============================================================================

class MockDevice:
    def __init__(self, width, height):
        self.width = width
        self.height = height
        self.closed = False
        self.root = None
        self.label = None
        self.photo = None
        self.latest_image = None  # Cache latest image
        self.running = True
        self._lock = threading.Lock()

    def setup_window(self):
        """Setup tkinter window - call from main thread only"""
        scale = 3
        window_width = self.width * scale
        window_height = self.height * scale
        self.root = tk.Tk()
        self.root.title("Screen Simulator")
        self.root.geometry(f"{window_width}x{window_height}")
        self.root.resizable(False, False)
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
        self.label = tk.Label(self.root, bg="black")
        self.label.pack(fill="both", expand=True)
        self._update_display()

    def _on_closing(self):
        self.running = False
        self.closed = True
        try:
            self.root.quit()
        except:
            pass

    def _update_display(self):
        """Update display with cached image - runs in main loop"""
        if self.running and self.root:
            try:
                # Get the latest cached image
                with self._lock:
                    pil_image = self.latest_image

                if pil_image is not None:
                    # Ensure image is in RGB mode
                    if pil_image.mode != 'RGB':
                        pil_image = pil_image.convert('RGB')
                    # Scale image 3x using nearest neighbor for crisp display
                    scale = 3
                    scaled_image = pil_image.resize(
                        (pil_image.width * scale, pil_image.height * scale),
                        Image.Resampling.NEAREST
                    )
                    self.photo = ImageTk.PhotoImage(scaled_image)
                    self.label.config(image=self.photo)
            except Exception as e:
                print(f"Display error: {e}")

            if self.running and self.root:
                self.root.after(50, self._update_display)

    def display(self, image):
        """Store latest image for display - can be called from any thread"""
        if not self.closed:
            with self._lock:
                self.latest_image = image

    def cleanup(self):
        print("Closing simulator window.")
        self.running = False
        self.closed = True
        if self.root:
            try:
                self.root.quit()
                self.root.destroy()
            except:
                pass

class MockBacklight:
    def __init__(self):
        self._value = 0.0

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, val):
        self._value = val
        print(f"Simulator: Backlight set to {val}")

# ============================================================================
# Hardware Initialization
# ============================================================================

if not SIMULATOR_MODE:
    print("Initializing luma.lcd SPI interface...")
    serial = spi(
        port=0,
        device=0,
        bus_speed_hz=52000000,
        transfer_size=4096,
        gpio_DC=24,
        gpio_RST=25,
    )
    print("Initializing ST7789 display...")
    device = st7789(serial, width=320, height=240)

    print("Initializing backlight PWM...")
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(18, GPIO.OUT)
    backlight = GPIO.PWM(18, 1000)   # hardware PWM, 1 kHz
    backlight.start(80)              # start at 80% duty cycle
    print("Hardware PWM backlight ON - 80% brightness")
else:
    device = MockDevice(width=320, height=240)
    backlight = MockBacklight()
    backlight.value = 0.8  # MockBacklight uses .value directly

_current_brightness: float = 0.8
_transition_thread: Optional[threading.Thread] = None
_transition_lock = threading.Lock()

def set_backlight(value: float) -> None:
    """Set backlight brightness instantly. value is 0.0–1.0."""
    global _current_brightness
    _current_brightness = max(0.0, min(1.0, value))
    if SIMULATOR_MODE:
        backlight.value = _current_brightness
    else:
        backlight.ChangeDutyCycle(_current_brightness * 100)

def transition_backlight(target: float, duration_sec: float) -> None:
    """Smoothly transition backlight to target brightness over duration_sec.
    Runs in a background thread; cancels any in-progress transition.
    """
    global _transition_thread

    target = max(0.0, min(1.0, target))

    def _run(target: float, duration: float) -> None:
        steps = max(1, int(duration * 50))   # ~50 steps/sec
        interval = duration / steps
        start = _current_brightness
        for i in range(1, steps + 1):
            # Check if we've been cancelled (a newer thread started)
            if threading.current_thread() is not _transition_thread:
                return
            val = start + (target - start) * (i / steps)
            set_backlight(val)
            time.sleep(interval)

    with _transition_lock:
        _transition_thread = threading.Thread(
            target=_run, args=(target, duration_sec), daemon=True
        )
        _transition_thread.start()

# ============================================================================
# Multi-Node Metrics Collector (MQTT)
# ============================================================================

# Maps structured topic suffix -> flat display key used by display_loop / get_node_info
_DISPLAY_KEY_MAP = {
    "cpu/usage":     "cpu_usage",
    "cpu/temp":      "cpu_temp",
    "cpu/freq":      "cpu_freq",
    "cpu/load_1":    "load_avg_1",
    "cpu/load_5":    "load_avg_5",
    "cpu/load_15":   "load_avg_15",
    "ram/percent":   "ram_usage",
    "disk/usage":    "disk_usage",
    "net/tx_rate":   "net_tx_rate",
    "net/rx_rate":   "net_rx_rate",
    "system/uptime": "uptime",
}


class MultiNodeCollector:
    """Collects system metrics from multiple RPi nodes via MQTT"""

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

        # Subscribe to all sub-topics for each node using wildcards
        for node_id, prefix in self.nodes.items():
            topic = f"{prefix}/#"
            self.client.subscribe(topic)
            print(f"Subscribed to {topic}")

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
            suffix  = parts[2]   # e.g. "cpu/usage", "net/eth0/bytes_sent"

            try:
                value = float(message.payload.decode())
            except (ValueError, UnicodeDecodeError):
                return   # skip non-numeric payloads

            with self._lock:
                if node_id not in self.latest_metrics:
                    return
                # Store under full suffix key (available for future use)
                self.latest_metrics[node_id][suffix] = value
                # Also store under flat display key if mapped
                display_key = _DISPLAY_KEY_MAP.get(suffix)
                if display_key:
                    self.latest_metrics[node_id][display_key] = value
        except Exception as e:
            print(f"Error processing MQTT message: {e}")

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

# ============================================================================
# Home Assistant MQTT Setup
# ============================================================================

mqtt_settings = Settings.MQTT(
    host=os.getenv("MQTT_HOST"),
    username=os.getenv("MQTT_USERNAME"),
    password=os.getenv("MQTT_PASSWORD")
)

device_info = DeviceInfo(name="Rack Dashboard", identifiers=["rack_dashboard_001"])

sensor_info = SensorInfo(
    name="CPU Temperature",
    device_class="temperature",
    unit_of_measurement="°C",
    unique_id="rack_cpu_temperature_001",
    device=device_info,
    expire_after=60
)

render_time_info = SensorInfo(
    name="Screen render time",
    unit_of_measurement="ms",
    unique_id="screen_render_time_001",
    device=device_info,
    expire_after=60
)

btn_next_info = ButtonInfo(
    name="Page Next",
    unique_id="page_next_001",
    device=device_info,
)

btn_prev_info = ButtonInfo(
    name="Page Previous",
    unique_id="page_prev_001",
    device=device_info,
)

light_info = LightInfo(
    name="Display Backlight",
    unique_id="display_backlight_001",
    device=device_info,
    supported_color_modes=["brightness"],
    expire_after=60
)

def backlight_callback(client: Client, user_data, message: MQTTMessage):
    """Handle backlight commands from Home Assistant"""
    if my_light is None:
        return
    try:
        payload = json.loads(message.payload.decode())
    except ValueError:
        print("Only JSON schema is supported for light entities!")
        return
    print(f"Received backlight command: {payload}")

    transition = float(payload.get("transition", 1.0))

    if "brightness" in payload:
        brightness = payload["brightness"]
        if 0 <= brightness <= 255:
            pwm_value = brightness / 255.0
            if transition > 0:
                transition_backlight(pwm_value, transition)
            else:
                set_backlight(pwm_value)
            print(f"Set backlight brightness to {brightness} ({pwm_value:.2f}), transition={transition}s")
            my_light.brightness(brightness)
        else:
            print("Brightness value must be between 0 and 255.")
    elif "state" in payload:
        if payload["state"] == light_info.payload_on:
            if transition > 0:
                transition_backlight(0.8, transition)
            else:
                set_backlight(0.8)
            my_light.on()
        else:
            if transition > 0:
                transition_backlight(0.0, transition)
            else:
                set_backlight(0.0)
            my_light.off()
    else:
        print("Unsupported command for backlight. Only 'brightness', 'state' or 'transition' are accepted.")

def _btn_next_callback(_client: Client, _user_data, _message: MQTTMessage) -> None:
    page_manager.next()

def _btn_prev_callback(_client: Client, _user_data, _message: MQTTMessage) -> None:
    page_manager.prev()

my_sensor = None
my_light = None
btn_next = None
btn_prev = None
render_time_sensor = None

try:
    settings = Settings(mqtt=mqtt_settings, entity=sensor_info)
    my_sensor = Sensor(settings)
    render_time_settings = Settings(mqtt=mqtt_settings, entity=render_time_info)
    render_time_sensor = Sensor(render_time_settings)
    btn_next_settings = Settings(mqtt=mqtt_settings, entity=btn_next_info)
    btn_next = Button(btn_next_settings, _btn_next_callback)
    btn_next.write_config()
    btn_prev_settings = Settings(mqtt=mqtt_settings, entity=btn_prev_info)
    btn_prev = Button(btn_prev_settings, _btn_prev_callback)
    btn_prev.write_config()
    light_settings = Settings(mqtt=mqtt_settings, entity=light_info)
    my_light = Light(light_settings, backlight_callback)
    my_light.off()
except Exception as e:
    print(f"Warning: Could not setup MQTT entities: {e}")
    print("Dashboard will run without MQTT Home Assistant integration")

# Shared render-time measurement (written by display_loop, read by sensor_loop)
_last_render_ms: Optional[float] = None
_render_ms_lock = threading.Lock()

# ============================================================================
# Multi-Node Setup
# ============================================================================

# Configure which nodes to display (3 nodes)
NODES = {
    "node1": "rack/node1",
    "node2": "rack/node2",
    "node3": "rack/node3",
}

# Initialize multi-node collector
node_collector = MultiNodeCollector(NODES)
mqtt_host = os.getenv("MQTT_HOST")
if mqtt_host:
    node_collector.setup(
        mqtt_host,
        os.getenv("MQTT_USERNAME"),
        os.getenv("MQTT_PASSWORD")
    )

# ============================================================================
# Simulator Mode - Mock Data Generator
# ============================================================================

import random

def generate_mock_data():
    """Generate realistic mock system metrics for simulator mode (node2 and node3 only)"""
    node_data = {
        "node2": {"base_cpu": 25, "base_ram": 40, "base_temp": 52},
        "node3": {"base_cpu": 55, "base_ram": 65, "base_temp": 45},
    }

    for node_id, base_values in node_data.items():
        # Add realistic variance to each metric
        cpu_usage = base_values["base_cpu"] + random.uniform(-10, 15)
        ram_usage = base_values["base_ram"] + random.uniform(-5, 8)
        cpu_temp = base_values["base_temp"] + random.uniform(-2, 3)
        cpu_freq = random.uniform(1700, 1950)

        # Inject into node collector's latest metrics
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
    print("Simulator mode: Generating mock node data every second")

# ============================================================================
# Display Setup
# ============================================================================

# ============================================================================
# Main Loop
# ============================================================================

def display_loop():
    """Main display update loop - renders 3 nodes with metrics and sparklines"""
    try:
        while True:
            # Get local system data and inject into node1
            local_data = display_consumer.get_all()
            if local_data.cpu_temp is not None or local_data.cpu_usage is not None or \
               local_data.ram_usage is not None or local_data.cpu_freq is not None:
                # Update node1 with local data
                local_data.node_id = "node1"
                node_collector.latest_metrics["node1"] = {
                    "cpu_usage":  local_data.cpu_usage,
                    "ram_usage":  local_data.ram_usage,
                    "cpu_temp":   local_data.cpu_temp,
                    "cpu_freq":   local_data.cpu_freq,
                    "uptime":     local_data.uptime,
                    "disk_usage": local_data.disk_usage,
                    "net_tx_rate": local_data.net_tx_rate,
                    "net_rx_rate": local_data.net_rx_rate,
                    "load_avg_1":  local_data.load_avg_1,
                    "load_avg_5":  local_data.load_avg_5,
                    "load_avg_15": local_data.load_avg_15,
                }

            # Generate mock data for node2 and node3 in simulator mode
            if SIMULATOR_MODE:
                generate_mock_data()

            # Build data dict for the active page
            page_data: Dict[str, Any] = {
                "node_ids": list(NODES.keys()),
                "detail_node": list(NODES.keys())[0],
            }
            for node_id in NODES.keys():
                node_info = node_collector.get_node_info(node_id)
                page_data[node_id] = {
                    "cpu_usage":   node_info.cpu_usage,
                    "ram_usage":   node_info.ram_usage,
                    "cpu_temp":    node_info.cpu_temp,
                    "cpu_freq":    node_info.cpu_freq,
                    "uptime":      node_info.uptime,
                    "disk_usage":  node_info.disk_usage,
                    "net_tx_rate": node_info.net_tx_rate,
                    "net_rx_rate": node_info.net_rx_rate,
                    "load_avg_1":  node_info.load_avg_1,
                    "load_avg_5":  node_info.load_avg_5,
                    "load_avg_15": node_info.load_avg_15,
                }

            # Keep histogram buffers populated regardless of which page is active
            for node_id in NODES.keys():
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
        print("Display loop interrupted")
        raise

def sensor_loop():
    """Update sensor values, send to Home Assistant, and publish node1 to MQTT"""
    try:
        last_ha_update = 0
        last_mqtt_publish = 0
        mqtt_client = None

        # Setup MQTT client for node1 publishing
        if mqtt_host:
            mqtt_client = Client()
            if os.getenv("MQTT_USERNAME") and os.getenv("MQTT_PASSWORD"):
                mqtt_client.username_pw_set(os.getenv("MQTT_USERNAME"), os.getenv("MQTT_PASSWORD"))
            try:
                mqtt_client.connect(mqtt_host, 1883, keepalive=60)
                mqtt_client.loop_start()
                print("Connected MQTT client for node1 publishing")
            except Exception as e:
                print(f"Failed to connect MQTT for publishing: {e}")
                mqtt_client = None

        while True:
            current_time = time.time()

            # Update Home Assistant every ~10 seconds
            if current_time - last_ha_update >= 10.0:
                sys_info = sensor_consumer.get_all()

                if my_sensor is not None and sys_info.cpu_temp is not None:
                    my_sensor.set_state(sys_info.cpu_temp)
                    print(f"Updated sensor value: {sys_info.cpu_temp:.2f}°C")

                with _render_ms_lock:
                    render_ms = _last_render_ms
                if render_time_sensor is not None and render_ms is not None:
                    render_time_sensor.set_state(round(render_ms, 1))
                    print(f"Screen render time: {render_ms:.1f}ms")

                last_ha_update = current_time

            # Publish node1 metrics to MQTT every 5 seconds
            if mqtt_client and current_time - last_mqtt_publish >= 5.0:
                sys_info = sensor_consumer.get_all()
                prefix = "rack/node1"

                def pub(topic, value):
                    mqtt_client.publish(f"{prefix}/{topic}", str(value), qos=0)

                # CPU
                if sys_info.cpu_usage        is not None: pub("cpu/usage",         sys_info.cpu_usage)
                if sys_info.cpu_temp         is not None: pub("cpu/temp",          sys_info.cpu_temp)
                if sys_info.cpu_freq         is not None: pub("cpu/freq",          sys_info.cpu_freq)
                if sys_info.cpu_freq_min     is not None: pub("cpu/freq_min",      sys_info.cpu_freq_min)
                if sys_info.cpu_freq_max     is not None: pub("cpu/freq_max",      sys_info.cpu_freq_max)
                if sys_info.cpu_count        is not None: pub("cpu/count",         sys_info.cpu_count)
                if sys_info.cpu_count_physical is not None: pub("cpu/count_physical", sys_info.cpu_count_physical)
                if sys_info.load_avg_1       is not None: pub("cpu/load_1",        sys_info.load_avg_1)
                if sys_info.load_avg_5       is not None: pub("cpu/load_5",        sys_info.load_avg_5)
                if sys_info.load_avg_15      is not None: pub("cpu/load_15",       sys_info.load_avg_15)
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
                for _attr in ("active", "inactive", "buffers", "cached", "shared", "slab"):
                    _v = getattr(sys_info, f"ram_{_attr}", None)
                    if _v is not None:
                        pub(f"ram/{_attr}", _v)

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
                    for _dev, _d in sys_info.disk_partitions.items():
                        for _k, _val in _d.items():
                            pub(f"disk/{_dev}/{_k}", _val)

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
                    for _iface, _d in sys_info.net_if_stats.items():
                        for _k, _val in _d.items():
                            pub(f"net/{_iface}/{_k}", _val)

                # System
                if sys_info.uptime is not None: pub("system/uptime", sys_info.uptime)

                # Fans
                if sys_info.fans:
                    for _fan_key, _speed in sys_info.fans.items():
                        pub(f"fans/{_fan_key}/speed", _speed)

                print("Published node1 metrics to MQTT")

                last_mqtt_publish = current_time

            time.sleep(5)
    except KeyboardInterrupt:
        print("Sensor loop interrupted")
        if mqtt_client:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        raise

# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    display_thread = threading.Thread(target=display_loop, daemon=True)
    sensor_thread = threading.Thread(target=sensor_loop, daemon=True)

    display_thread.start()
    sensor_thread.start()

    try:
        if SIMULATOR_MODE:
            import signal
            sim: MockDevice = device  # type: ignore[assignment]

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
        print("\nExiting...")
        sys_info_producer.stop()
        if not SIMULATOR_MODE:
            set_backlight(0.0)
            backlight.stop()
            print("Backlight OFF")
            device.cleanup()
            print("Display cleaned up")
            GPIO.cleanup()
        else:
            device.cleanup()
        exit(0)
