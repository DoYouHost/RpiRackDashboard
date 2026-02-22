"""Home Assistant MQTT entities: CPU sensor, display backlight light, page navigation buttons."""

import json
import logging
from dataclasses import dataclass
from typing import Callable, Optional

from ha_mqtt_discoverable import Settings, DeviceInfo
from ha_mqtt_discoverable.sensors import Button, ButtonInfo, Light, LightInfo, Sensor, SensorInfo
from paho.mqtt.client import Client, MQTTMessage

logger = logging.getLogger(__name__)


@dataclass
class HAEntities:
    sensor: Optional[Sensor] = None
    render_time_sensor: Optional[Sensor] = None
    light: Optional[Light] = None
    btn_next: Optional[Button] = None
    btn_prev: Optional[Button] = None


def setup_ha_entities(
    mqtt_host: str,
    username: Optional[str],
    password: Optional[str],
    set_backlight_fn: Callable[[float], None],
    transition_backlight_fn: Callable[[float, float], None],
    page_next_fn: Callable[[], None],
    page_prev_fn: Callable[[], None],
) -> HAEntities:
    """Initialize all Home Assistant MQTT entities.

    Args:
        mqtt_host: MQTT broker hostname.
        username: MQTT username (optional).
        password: MQTT password (optional).
        set_backlight_fn: Callable(brightness_0_1) to set backlight instantly.
        transition_backlight_fn: Callable(target_0_1, duration_sec) for smooth transitions.
        page_next_fn: Callable to advance to next dashboard page.
        page_prev_fn: Callable to go to previous dashboard page.

    Returns:
        HAEntities with created entities (fields are None for any that failed).
    """
    entities = HAEntities()
    mqtt_settings = Settings.MQTT(host=mqtt_host, username=username, password=password)
    device_info = DeviceInfo(name="Rack Dashboard", identifiers=["rack_dashboard_001"])

    try:
        light_info = LightInfo(
            name="Display Backlight",
            unique_id="display_backlight_001",
            device=device_info,
            supported_color_modes=["brightness"],
            expire_after=60,
        )

        def backlight_callback(client: Client, user_data, message: MQTTMessage) -> None:
            if entities.light is None:
                return
            try:
                payload = json.loads(message.payload.decode())
            except ValueError:
                logger.warning("Only JSON schema is supported for light entities!")
                return
            logger.debug("Received backlight command: %s", payload)
            transition = float(payload.get("transition", 1.0))
            if "brightness" in payload:
                brightness = payload["brightness"]
                if 0 <= brightness <= 255:
                    pwm_value = brightness / 255.0
                    if transition > 0:
                        transition_backlight_fn(pwm_value, transition)
                    else:
                        set_backlight_fn(pwm_value)
                    logger.info("Set backlight brightness to %s (%.2f), transition=%ss", brightness, pwm_value, transition)
                    entities.light.brightness(brightness)
                else:
                    logger.warning("Brightness value must be between 0 and 255.")
            elif "state" in payload:
                if payload["state"] == light_info.payload_on:
                    if transition > 0:
                        transition_backlight_fn(0.8, transition)
                    else:
                        set_backlight_fn(0.8)
                    entities.light.on()
                else:
                    if transition > 0:
                        transition_backlight_fn(0.0, transition)
                    else:
                        set_backlight_fn(0.0)
                    entities.light.off()
            else:
                logger.warning("Unsupported command for backlight. Only 'brightness', 'state' or 'transition' are accepted.")

        def _btn_next_callback(_client: Client, _user_data, _message: MQTTMessage) -> None:
            page_next_fn()

        def _btn_prev_callback(_client: Client, _user_data, _message: MQTTMessage) -> None:
            page_prev_fn()

        # CPU Temperature sensor
        sensor_info = SensorInfo(
            name="CPU Temperature",
            device_class="temperature",
            unit_of_measurement="°C",
            unique_id="rack_cpu_temperature_001",
            device=device_info,
            expire_after=60,
        )
        entities.sensor = Sensor(Settings(mqtt=mqtt_settings, entity=sensor_info))

        # Screen render time sensor
        render_time_info = SensorInfo(
            name="Screen render time",
            unit_of_measurement="ms",
            unique_id="screen_render_time_001",
            device=device_info,
            expire_after=60,
        )
        entities.render_time_sensor = Sensor(Settings(mqtt=mqtt_settings, entity=render_time_info))

        # Page navigation buttons
        btn_next_info = ButtonInfo(name="Page Next", unique_id="page_next_001", device=device_info)
        entities.btn_next = Button(Settings(mqtt=mqtt_settings, entity=btn_next_info), _btn_next_callback)
        entities.btn_next.write_config()

        btn_prev_info = ButtonInfo(name="Page Previous", unique_id="page_prev_001", device=device_info)
        entities.btn_prev = Button(Settings(mqtt=mqtt_settings, entity=btn_prev_info), _btn_prev_callback)
        entities.btn_prev.write_config()

        # Backlight light entity (created last so entities.light is set before callbacks fire)
        entities.light = Light(Settings(mqtt=mqtt_settings, entity=light_info), backlight_callback)
        entities.light.off()

    except Exception as e:
        logger.warning("Could not setup MQTT entities: %s", e)
        logger.warning("Dashboard will run without MQTT Home Assistant integration")

    return entities
