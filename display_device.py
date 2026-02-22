"""Display hardware abstraction: real ST7789 + Tkinter simulator (MockDevice/MockBacklight)."""

import logging
import sys
import threading
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# Simulator Classes
# ============================================================================

class MockDevice:
    def __init__(self, width, height):
        self.width = width
        self.height = height
        self.closed = False
        self.root = None
        self.label = None
        self.photo = None
        self.latest_image = None
        self.running = True
        self._lock = threading.Lock()

    def setup_window(self):
        """Setup tkinter window - call from main thread only."""
        import tkinter as tk
        scale = 3
        self.root = tk.Tk()
        self.root.title("Screen Simulator")
        self.root.geometry(f"{self.width * scale}x{self.height * scale}")
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
        except Exception:
            pass

    def _update_display(self):
        """Update display with cached image - runs in main loop."""
        if self.running and self.root:
            try:
                from PIL import Image, ImageTk
                with self._lock:
                    pil_image = self.latest_image
                if pil_image is not None:
                    if pil_image.mode != "RGB":
                        pil_image = pil_image.convert("RGB")
                    scale = 3
                    scaled_image = pil_image.resize(
                        (pil_image.width * scale, pil_image.height * scale),
                        Image.Resampling.NEAREST,
                    )
                    self.photo = ImageTk.PhotoImage(scaled_image)
                    self.label.config(image=self.photo)
            except Exception as e:
                logger.error("Display error: %s", e)

            if self.running and self.root:
                self.root.after(50, self._update_display)

    def display(self, image):
        """Store latest image for display - can be called from any thread."""
        if not self.closed:
            with self._lock:
                self.latest_image = image

    def cleanup(self):
        logger.info("Closing simulator window.")
        self.running = False
        self.closed = True
        if self.root:
            try:
                self.root.quit()
                self.root.destroy()
            except Exception:
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
        logger.debug("Simulator: Backlight set to %s", val)


# ============================================================================
# Module-level state (initialized by init_device)
# ============================================================================

_simulator_mode: bool = False
_device: Any = None
_backlight: Any = None
_current_brightness: float = 0.8
_transition_thread: Optional[threading.Thread] = None
_transition_lock = threading.Lock()


# ============================================================================
# Public API
# ============================================================================

def init_device(simulator_mode: bool):
    """Initialize display device and backlight.

    Args:
        simulator_mode: If True, use Tkinter simulator. If False, initialize real hardware.

    Returns:
        Tuple of (device, backlight).
    """
    global _simulator_mode, _device, _backlight, _current_brightness
    _simulator_mode = simulator_mode

    if not simulator_mode:
        try:
            from luma.lcd.device import st7789
            from luma.core.interface.serial import spi
            import RPi.GPIO as GPIO
        except ImportError as e:
            logger.error(
                "FATAL: Hardware libraries not available: %s\n"
                "SIMULATOR_MODE=false was set but hardware is not available!\n"
                "Either:\n"
                "  1. Install missing libraries (pip install -r requirements.txt)\n"
                "  2. Set SIMULATOR_MODE=true if running in simulator",
                e,
            )
            sys.exit(1)

        logger.info("Initializing luma.lcd SPI interface...")
        serial = spi(
            port=0,
            device=0,
            bus_speed_hz=52000000,
            transfer_size=4096,
            gpio_DC=24,
            gpio_RST=25,
        )
        logger.info("Initializing ST7789 display...")
        _device = st7789(serial, width=320, height=240)

        logger.info("Initializing backlight PWM...")
        GPIO.setmode(GPIO.BCM)
        GPIO.setup(18, GPIO.OUT)
        _backlight = GPIO.PWM(18, 1000)
        _backlight.start(80)
        _current_brightness = 0.8
        logger.info("Hardware PWM backlight ON - 80%% brightness")
    else:
        _device = MockDevice(width=320, height=240)
        _backlight = MockBacklight()
        _backlight.value = 0.8
        _current_brightness = 0.8

    return _device, _backlight


def set_backlight(value: float) -> None:
    """Set backlight brightness instantly. value is 0.0–1.0."""
    global _current_brightness
    _current_brightness = max(0.0, min(1.0, value))
    if _simulator_mode:
        _backlight.value = _current_brightness
    else:
        _backlight.ChangeDutyCycle(_current_brightness * 100)


def transition_backlight(target: float, duration_sec: float) -> None:
    """Smoothly transition backlight to target brightness over duration_sec.

    Runs in a background thread; cancels any in-progress transition.
    """
    global _transition_thread
    target = max(0.0, min(1.0, target))

    def _run(target: float, duration: float) -> None:
        steps = max(1, int(duration * 50))
        interval = duration / steps
        start = _current_brightness
        for i in range(1, steps + 1):
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


def cleanup_device() -> None:
    """Clean up display hardware resources. Call before exit."""
    if not _simulator_mode:
        set_backlight(0.0)
        if _backlight is not None:
            _backlight.stop()
        logger.info("Backlight OFF")
        if _device is not None:
            _device.cleanup()
        logger.info("Display cleaned up")
        try:
            import RPi.GPIO as GPIO
            GPIO.cleanup()
        except Exception:
            pass
    else:
        if _device is not None:
            _device.cleanup()
