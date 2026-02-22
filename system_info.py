"""System information gathering module with producer-consumer pattern and multi-node support"""

import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any
from collections import deque
import threading
import queue
import socket
import psutil
import time

logger = logging.getLogger(__name__)


@dataclass
class SystemInfo:
    """Standardized system information container"""
    node_id: str = "local"

    # ── CPU ──────────────────────────────────────────────────────
    cpu_temp:    Optional[float] = None
    cpu_usage:   Optional[float] = None
    cpu_freq:    Optional[float] = None   # current MHz
    load_avg_1:  Optional[float] = None
    load_avg_5:  Optional[float] = None
    load_avg_15: Optional[float] = None
    cpu_freq_min:        Optional[float] = None
    cpu_freq_max:        Optional[float] = None
    cpu_count:           Optional[int]   = None   # logical
    cpu_count_physical:  Optional[int]   = None
    cpu_ctx_switches:    Optional[int]   = None
    cpu_interrupts:      Optional[int]   = None
    cpu_soft_interrupts: Optional[int]   = None
    cpu_syscalls:        Optional[int]   = None

    # ── RAM ─────────────────────────────────────────────────────
    ram_usage:     Optional[float] = None
    ram_total:     Optional[int] = None
    ram_available: Optional[int] = None
    ram_used:      Optional[int] = None
    ram_free:      Optional[int] = None
    ram_active:    Optional[int] = None
    ram_inactive:  Optional[int] = None
    ram_buffers:   Optional[int] = None
    ram_cached:    Optional[int] = None
    ram_shared:    Optional[int] = None
    ram_slab:      Optional[int] = None

    # ── Swap ───────────────────────────────────────────────────────────
    swap_percent: Optional[float] = None
    swap_total:   Optional[int]   = None
    swap_used:    Optional[int]   = None
    swap_free:    Optional[int]   = None
    swap_sin:     Optional[int]   = None
    swap_sout:    Optional[int]   = None

    # ── Disk ────────────────────────────────────────────────────
    disk_usage:   Optional[float] = None   # root %
    disk_io_read_bytes:  Optional[int] = None
    disk_io_write_bytes: Optional[int] = None
    disk_io_read_count:  Optional[int] = None
    disk_io_write_count: Optional[int] = None
    disk_io_read_time:   Optional[int] = None
    disk_io_write_time:  Optional[int] = None
    disk_partitions: Optional[Dict[str, dict]] = None  # {dev: {usage_percent,total,used,free}}

    # ── Net ─────────────────────────────────────────────────────
    net_tx_rate:                 Optional[float] = None   # bytes/sec (delta)
    net_rx_rate:                 Optional[float] = None   # bytes/sec (delta)
    net_bytes_sent:              Optional[int] = None
    net_bytes_recv:              Optional[int] = None
    net_packets_sent:            Optional[int] = None
    net_packets_recv:            Optional[int] = None
    net_errin:                   Optional[int] = None
    net_errout:                  Optional[int] = None
    net_dropin:                  Optional[int] = None
    net_dropout:                 Optional[int] = None
    net_connections:             Optional[int] = None
    net_connections_established: Optional[int] = None
    net_connections_listen:      Optional[int] = None
    net_if_stats: Optional[Dict[str, dict]] = None  # {iface: {speed,mtu,isup,bytes_sent,...}}

    # ── Fans ───────────────────────────────────────────────────────────
    fans: Optional[Dict[str, float]] = None   # {fan_name: rpm}
    
    # ── Sytstem ───────────────────────────────────────────────────────────
    uptime:       Optional[float] = None   # boot_time

    timestamp: float = field(default_factory=time.time)

    def __str__(self) -> str:
        """Return formatted string representation"""
        lines = []
        if self.cpu_temp is not None:
            lines.append(f"CPU Temp: {self.cpu_temp:.1f}°C")
        if self.cpu_usage is not None:
            lines.append(f"CPU Use: {self.cpu_usage:.1f}%")
        if self.ram_usage is not None:
            lines.append(f"RAM: {self.ram_usage:.1f}%")
        if self.cpu_freq is not None:
            lines.append(f"CPU Freq: {self.cpu_freq:.0f} MHz")
        if self.disk_usage is not None:
            lines.append(f"Disk: {self.disk_usage:.1f}%")
        if self.net_tx_rate is not None and self.net_rx_rate is not None:
            lines.append(f"Net TX: {self.net_tx_rate:.0f} B/s  RX: {self.net_rx_rate:.0f} B/s")
        if self.load_avg_1 is not None:
            lines.append(f"Load: {self.load_avg_1:.2f} {self.load_avg_5:.2f} {self.load_avg_15:.2f}")
        return "\n".join(lines)

    def to_dict(self) -> dict:
        """Return as dictionary"""
        return {
            "cpu_temp": self.cpu_temp,
            "cpu_usage": self.cpu_usage,
            "ram_usage": self.ram_usage,
            "cpu_freq": self.cpu_freq,
            "uptime": self.uptime,
            "disk_usage": self.disk_usage,
            "net_tx_rate": self.net_tx_rate,
            "net_rx_rate": self.net_rx_rate,
            "load_avg_1": self.load_avg_1,
            "load_avg_5": self.load_avg_5,
            "load_avg_15": self.load_avg_15,
            # CPU extras
            "cpu_freq_min": self.cpu_freq_min,
            "cpu_freq_max": self.cpu_freq_max,
            "cpu_count": self.cpu_count,
            "cpu_count_physical": self.cpu_count_physical,
            "cpu_ctx_switches": self.cpu_ctx_switches,
            "cpu_interrupts": self.cpu_interrupts,
            "cpu_soft_interrupts": self.cpu_soft_interrupts,
            "cpu_syscalls": self.cpu_syscalls,
            # RAM extras
            "ram_total": self.ram_total,
            "ram_available": self.ram_available,
            "ram_used": self.ram_used,
            "ram_free": self.ram_free,
            "ram_active": self.ram_active,
            "ram_inactive": self.ram_inactive,
            "ram_buffers": self.ram_buffers,
            "ram_cached": self.ram_cached,
            "ram_shared": self.ram_shared,
            "ram_slab": self.ram_slab,
            # Swap
            "swap_percent": self.swap_percent,
            "swap_total": self.swap_total,
            "swap_used": self.swap_used,
            "swap_free": self.swap_free,
            "swap_sin": self.swap_sin,
            "swap_sout": self.swap_sout,
            # Disk extras
            "disk_io_read_bytes": self.disk_io_read_bytes,
            "disk_io_write_bytes": self.disk_io_write_bytes,
            "disk_io_read_count": self.disk_io_read_count,
            "disk_io_write_count": self.disk_io_write_count,
            "disk_io_read_time": self.disk_io_read_time,
            "disk_io_write_time": self.disk_io_write_time,
            "disk_partitions": self.disk_partitions,
            # Net extras
            "net_bytes_sent": self.net_bytes_sent,
            "net_bytes_recv": self.net_bytes_recv,
            "net_packets_sent": self.net_packets_sent,
            "net_packets_recv": self.net_packets_recv,
            "net_errin": self.net_errin,
            "net_errout": self.net_errout,
            "net_dropin": self.net_dropin,
            "net_dropout": self.net_dropout,
            "net_connections": self.net_connections,
            "net_connections_established": self.net_connections_established,
            "net_connections_listen": self.net_connections_listen,
            "net_if_stats": self.net_if_stats,
            # Fans
            "fans": self.fans,
        }


class ConsumerHandle:
    """Handle for independent queue access when subscribed to SystemInfoProducer"""

    def __init__(self):
        self.queue = queue.Queue()

    def get_all(self) -> "SystemInfo":
        """Drain own queue and return averaged SystemInfo.

        Returns:
            SystemInfo with averaged values across all queued samples.
            Note: counters and non-averaged fields return the latest value.
        """
        data_list = []
        while True:
            try:
                data_list.append(self.queue.get_nowait())
            except queue.Empty:
                break

        if not data_list:
            return SystemInfo()

        def avg(values):
            return sum(values) / len(values) if values else None

        last = data_list[-1]

        return SystemInfo(
            # ── CPU averaged ─────────────────────────────────────────────────
            cpu_temp=avg([d.cpu_temp for d in data_list if d.cpu_temp is not None]),
            cpu_usage=avg([d.cpu_usage for d in data_list if d.cpu_usage is not None]),
            ram_usage=avg([d.ram_usage for d in data_list if d.ram_usage is not None]),
            cpu_freq=avg([d.cpu_freq for d in data_list if d.cpu_freq is not None]),
            disk_usage=avg([d.disk_usage for d in data_list if d.disk_usage is not None]),
            net_tx_rate=avg([d.net_tx_rate for d in data_list if d.net_tx_rate is not None]),
            net_rx_rate=avg([d.net_rx_rate for d in data_list if d.net_rx_rate is not None]),
            # ── take latest ──────────────────────────────────────────────────
            uptime=last.uptime,
            load_avg_1=last.load_avg_1,
            load_avg_5=last.load_avg_5,
            load_avg_15=last.load_avg_15,
            # CPU extras
            cpu_freq_min=last.cpu_freq_min,
            cpu_freq_max=last.cpu_freq_max,
            cpu_count=last.cpu_count,
            cpu_count_physical=last.cpu_count_physical,
            cpu_ctx_switches=last.cpu_ctx_switches,
            cpu_interrupts=last.cpu_interrupts,
            cpu_soft_interrupts=last.cpu_soft_interrupts,
            cpu_syscalls=last.cpu_syscalls,
            # RAM extras
            ram_total=last.ram_total,
            ram_available=last.ram_available,
            ram_used=last.ram_used,
            ram_free=last.ram_free,
            ram_active=last.ram_active,
            ram_inactive=last.ram_inactive,
            ram_buffers=last.ram_buffers,
            ram_cached=last.ram_cached,
            ram_shared=last.ram_shared,
            ram_slab=last.ram_slab,
            # Swap
            swap_percent=last.swap_percent,
            swap_total=last.swap_total,
            swap_used=last.swap_used,
            swap_free=last.swap_free,
            swap_sin=last.swap_sin,
            swap_sout=last.swap_sout,
            # Disk extras
            disk_io_read_bytes=last.disk_io_read_bytes,
            disk_io_write_bytes=last.disk_io_write_bytes,
            disk_io_read_count=last.disk_io_read_count,
            disk_io_write_count=last.disk_io_write_count,
            disk_io_read_time=last.disk_io_read_time,
            disk_io_write_time=last.disk_io_write_time,
            disk_partitions=last.disk_partitions,
            # Net extras
            net_bytes_sent=last.net_bytes_sent,
            net_bytes_recv=last.net_bytes_recv,
            net_packets_sent=last.net_packets_sent,
            net_packets_recv=last.net_packets_recv,
            net_errin=last.net_errin,
            net_errout=last.net_errout,
            net_dropin=last.net_dropin,
            net_dropout=last.net_dropout,
            net_connections=last.net_connections,
            net_connections_established=last.net_connections_established,
            net_connections_listen=last.net_connections_listen,
            net_if_stats=last.net_if_stats,
            # Fans
            fans=last.fans,
        )


class NodeMetricsHistory:
    """Tracks 1-hour rolling window of metrics per node for sparkline rendering"""

    def __init__(self, max_samples: int = 3600):
        """
        Initialize history tracker.

        Args:
            max_samples: Maximum samples to keep (1 per second = 3600 for 1 hour)
        """
        self.max_samples = max_samples
        self.history: Dict[str, Dict[str, deque]] = {}
        self._lock = threading.Lock()

    def update(self, sys_info: "SystemInfo") -> None:
        """Record a new measurement for a node.

        Args:
            sys_info: SystemInfo with node_id and metrics
        """
        node_id = sys_info.node_id
        with self._lock:
            if node_id not in self.history:
                self.history[node_id] = {
                    "cpu_usage": deque(maxlen=self.max_samples),
                    "ram_usage": deque(maxlen=self.max_samples),
                    "cpu_temp": deque(maxlen=self.max_samples),
                }

            # Add values, skip None
            if sys_info.cpu_usage is not None:
                self.history[node_id]["cpu_usage"].append(sys_info.cpu_usage)
            if sys_info.ram_usage is not None:
                self.history[node_id]["ram_usage"].append(sys_info.ram_usage)
            if sys_info.cpu_temp is not None:
                self.history[node_id]["cpu_temp"].append(sys_info.cpu_temp)

    def get_sparkline(self, node_id: str, metric: str, width: int = 30) -> List[int]:
        """Get sparkline data for a metric, downsampled to fit display width.

        Args:
            node_id: Node identifier
            metric: "cpu_usage", "ram_usage", or "cpu_temp"
            width: Target width in pixels/characters for sparkline

        Returns:
            List of values scaled to 0-100 for sparkline rendering
        """
        with self._lock:
            if node_id not in self.history or metric not in self.history[node_id]:
                return []

            data = list(self.history[node_id][metric])
            if not data:
                return []

            # Downsample if needed
            if len(data) > width:
                step = len(data) // width
                downsampled = [data[i * step] for i in range(width)]
            else:
                downsampled = data

            # Normalize to 0-100
            min_val = min(downsampled) if downsampled else 0
            max_val = max(downsampled) if downsampled else 1
            range_val = max_val - min_val if max_val > min_val else 1

            normalized = [int(100 * (v - min_val) / range_val) for v in downsampled]
            return normalized


# ── Network rate state ────────────────────────────────────────────────────────
# Persists between calls to compute bytes/sec deltas.
_prev_net_counters: Optional[Any] = None
_prev_net_time: Optional[float] = None


def get_system_info(node_id: str = "local") -> "SystemInfo":
    """Gather all system information into a standardized format (synchronous).

    Args:
        node_id: Identifier for this node (default: "local")
    """
    info = SystemInfo(node_id=node_id)

    # CPU Temperature
    try:
        temps = psutil.sensors_temperatures()
        if 'coretemp' in temps:
            info.cpu_temp = max([t.current for t in temps['coretemp']])
        else:
            with open('/sys/class/thermal/thermal_zone0/temp', 'r') as f:
                info.cpu_temp = int(f.read()) / 1000.0
    except:
        pass

    # CPU Usage
    try:
        info.cpu_usage = psutil.cpu_percent(interval=0.1)
    except:
        pass

    # RAM Usage
    try:
        info.ram_usage = psutil.virtual_memory().percent
    except:
        pass

    # CPU Frequency (current)
    try:
        info.cpu_freq = psutil.cpu_freq().current
    except:
        pass

    # CPU Frequency min/max + count + stats
    try:
        f = psutil.cpu_freq()
        if f:
            info.cpu_freq_min = f.min
            info.cpu_freq_max = f.max
    except:
        pass
    try:
        info.cpu_count = psutil.cpu_count(logical=True)
        info.cpu_count_physical = psutil.cpu_count(logical=False)
    except:
        pass
    try:
        s = psutil.cpu_stats()
        info.cpu_ctx_switches    = s.ctx_switches
        info.cpu_interrupts      = s.interrupts
        info.cpu_soft_interrupts = s.soft_interrupts
        info.cpu_syscalls        = s.syscalls
    except:
        pass

    # Uptime (in seconds)
    try:
        info.uptime = psutil.boot_time()
    except:
        pass

    # Disk usage (root filesystem)
    try:
        info.disk_usage = psutil.disk_usage('/').percent
    except:
        pass

    # Disk IO counters
    try:
        dio = psutil.disk_io_counters()
        if dio:
            info.disk_io_read_bytes  = dio.read_bytes
            info.disk_io_write_bytes = dio.write_bytes
            info.disk_io_read_count  = dio.read_count
            info.disk_io_write_count = dio.write_count
            info.disk_io_read_time   = dio.read_time
            info.disk_io_write_time  = dio.write_time
    except:
        pass

    # Disk partitions (physical only)
    try:
        parts = {}
        for part in psutil.disk_partitions(all=False):
            dev = part.device.lstrip('/').replace('dev/', '', 1).replace('/', '_')
            try:
                u = psutil.disk_usage(part.mountpoint)
                parts[dev] = dict(
                    usage_percent=u.percent,
                    total=u.total,
                    used=u.used,
                    free=u.free,
                )
            except:
                pass
        if parts:
            info.disk_partitions = parts
    except:
        pass

    # RAM extras
    try:
        vm = psutil.virtual_memory()
        info.ram_total     = vm.total
        info.ram_available = vm.available
        info.ram_used      = vm.used
        info.ram_free      = vm.free
        for attr in ("active", "inactive", "buffers", "cached", "shared", "slab"):
            if hasattr(vm, attr):
                setattr(info, f"ram_{attr}", getattr(vm, attr))
    except:
        pass

    # Swap
    try:
        sw = psutil.swap_memory()
        info.swap_percent = sw.percent
        info.swap_total   = sw.total
        info.swap_used    = sw.used
        info.swap_free    = sw.free
        info.swap_sin     = sw.sin
        info.swap_sout    = sw.sout
    except:
        pass

    # Network TX/RX rates (bytes/sec) — computed as delta from previous sample
    global _prev_net_counters, _prev_net_time
    try:
        now = time.monotonic()
        counters = psutil.net_io_counters()
        if _prev_net_counters is not None and _prev_net_time is not None:
            dt = now - _prev_net_time
            if dt > 0:
                info.net_tx_rate = (counters.bytes_sent - _prev_net_counters.bytes_sent) / dt
                info.net_rx_rate = (counters.bytes_recv - _prev_net_counters.bytes_recv) / dt
        _prev_net_counters = counters
        _prev_net_time = now
    except:
        pass

    # Net IO counters (cumulative)
    try:
        io = psutil.net_io_counters()
        info.net_bytes_sent   = io.bytes_sent
        info.net_bytes_recv   = io.bytes_recv
        info.net_packets_sent = io.packets_sent
        info.net_packets_recv = io.packets_recv
        info.net_errin        = io.errin
        info.net_errout       = io.errout
        info.net_dropin       = io.dropin
        info.net_dropout      = io.dropout
    except:
        pass

    # Net connections
    try:
        conns = psutil.net_connections()
        info.net_connections             = len(conns)
        info.net_connections_established = sum(1 for c in conns if c.status == 'ESTABLISHED')
        info.net_connections_listen      = sum(1 for c in conns if c.status == 'LISTEN')
    except:
        pass

    # Per-interface stats + per-nic IO
    try:
        if_stats = psutil.net_if_stats()
        per_nic  = psutil.net_io_counters(pernic=True)
        ifaces: Dict[str, dict] = {}
        for iface, s in if_stats.items():
            key = iface.replace('/', '_')
            ifaces[key] = dict(speed=s.speed, mtu=s.mtu, isup=int(s.isup))
            if iface in per_nic:
                n = per_nic[iface]
                ifaces[key].update(
                    bytes_sent=n.bytes_sent,
                    bytes_recv=n.bytes_recv,
                    packets_sent=n.packets_sent,
                    packets_recv=n.packets_recv,
                )
        if ifaces:
            info.net_if_stats = ifaces
    except:
        pass

    # Load averages (1, 5, 15 min)
    try:
        load1, load5, load15 = psutil.getloadavg()
        info.load_avg_1  = load1
        info.load_avg_5  = load5
        info.load_avg_15 = load15
    except:
        pass

    # Fans
    try:
        fan_data = psutil.sensors_fans()
        if fan_data:
            fans: Dict[str, float] = {}
            for name, fan_list in fan_data.items():
                for i, fan in enumerate(fan_list):
                    key = f"{name}_{i}" if len(fan_list) > 1 else name
                    fans[key] = fan.current
            if fans:
                info.fans = fans
    except:
        pass

    return info


class SystemInfoProducer:
    """
    Producer that gathers system information at regular intervals and broadcasts to subscribers.
    Each subscriber gets its own independent queue for calculating averages.
    """

    def __init__(self, update_interval: float = 1.0):
        """
        Initialize the producer

        Args:
            update_interval: Time between updates in seconds (default 1.0)
        """
        self.update_interval = update_interval
        self._subscribers = []  # List of ConsumerHandle queues
        self._subscribers_lock = threading.Lock()
        self.running = False
        self.thread = None

    def start(self) -> None:
        """Start the producer thread"""
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._produce, daemon=True)
            self.thread.start()

    def stop(self) -> None:
        """Stop the producer thread"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=2)

    def subscribe(self) -> ConsumerHandle:
        """Register a new consumer and return its independent handle.

        Returns:
            ConsumerHandle with its own queue for receiving updates
        """
        handle = ConsumerHandle()
        with self._subscribers_lock:
            self._subscribers.append(handle.queue)
        return handle

    def unsubscribe(self, handle: ConsumerHandle) -> None:
        """Unregister a consumer handle.

        Args:
            handle: ConsumerHandle to remove
        """
        with self._subscribers_lock:
            if handle.queue in self._subscribers:
                self._subscribers.remove(handle.queue)

    def _produce(self) -> None:
        """Producer loop - gathers data every interval and broadcasts to all subscribers"""
        while self.running:
            try:
                sys_info = get_system_info()
                # Broadcast to all subscriber queues
                with self._subscribers_lock:
                    for subscriber_queue in self._subscribers:
                        subscriber_queue.put_nowait(sys_info)
            except Exception as e:
                logger.error("Error in producer: %s", e)

            # Sleep for the update interval
            for _ in range(int(self.update_interval * 10)):
                if not self.running:
                    break
                threading.Event().wait(0.1)
