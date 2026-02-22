# node_agent — Setup Guide

`node_agent.py` runs on a headless Raspberry Pi node. It collects local system metrics and publishes them to an MQTT broker every few seconds. No display or Home Assistant integration — just metrics publishing.

Metrics are published to `rack/<NODE_ID>/<metric>` (e.g. `rack/node2/cpu_temp`).

---

## Requirements

- Python 3.8+
- Git
- Network access to the MQTT broker

---

## 1. Clone the repository

```bash
git clone https://github.com/DoYouHost/RpiRackDashboard.git ~/RpiRackDashboard
cd ~/RpiRackDashboard
```

---

## 2. Create a virtual environment and install dependencies

```bash
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install .
```

> This installs all dependencies from `pyproject.toml`. The `[rpi]` extras (`luma.lcd`, `RPi.GPIO`, etc.) are **not** included in the base install and are not needed — `node_agent` has no display. Only install them on the display node with `pip install ".[rpi]"`.

---

## 3. Create a `.env` file

```bash
nano ~/RpiRackDashboard/.env
```

```ini
MQTT_HOST=192.168.1.10
MQTT_USERNAME=mqtt_user
MQTT_PASSWORD=mqtt_pass
```

| Variable        | Required | Description                        |
|----------------|----------|------------------------------------|
| `MQTT_HOST`     | Yes      | IP or hostname of the MQTT broker  |
| `MQTT_USERNAME` | No       | MQTT username (omit if no auth)    |
| `MQTT_PASSWORD` | No       | MQTT password (omit if no auth)    |

---

## 4. Test manually

```bash
cd ~/RpiRackDashboard
.venv/bin/python node_agent.py --node node2
```

Optional flags:

```
--node NODE_ID      Node identifier, e.g. node1, node2 (required)
--interval SECONDS  Publish interval in seconds (default: 5.0)
--debug             Enable DEBUG level logging
```

Example with debug output:

```bash
cd ~/RpiRackDashboard
.venv/bin/python node_agent.py --node node2 --interval 10 --debug
```

Expected output:

```
2026-02-22 17:00:01  INFO      node_agent        Node agent starting: node='node2', topic prefix='rack/node2', interval=5.0s
2026-02-22 17:00:01  INFO      node_agent        Connected to MQTT broker at 192.168.1.10
2026-02-22 17:00:01  INFO      node_agent        Publishing metrics to rack/node2/... — press Ctrl+C to stop
```

---

## 5. Configure as a systemd service

### Create the service file

```bash
sudo nano /etc/systemd/system/node-agent.service
```

```ini
[Unit]
Description=RpiRackDashboard Node Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi/RpiRackDashboard
ExecStart=/home/pi/RpiRackDashboard/.venv/bin/python node_agent.py --node node2
Restart=on-failure
RestartSec=10

# Environment variables — use these instead of .env if preferred
# Environment=MQTT_HOST=192.168.1.10
# Environment=MQTT_USERNAME=mqtt_user
# Environment=MQTT_PASSWORD=mqtt_pass

# Or load from .env file:
EnvironmentFile=/home/pi/RpiRackDashboard/.env

StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

> Adjust `User`, `WorkingDirectory`, `ExecStart`, and `--node` to match your setup.

### Enable and start

```bash
sudo systemctl daemon-reload
sudo systemctl enable node-agent.service
sudo systemctl start node-agent.service
```

### Check status and logs

```bash
sudo systemctl status node-agent.service
journalctl -u node-agent.service -f
```

### Change log level at runtime

Set `LOG_LEVEL` in the `.env` file or the service `Environment=` line:

```ini
LOG_LEVEL=DEBUG
```

Then reload:

```bash
sudo systemctl restart node-agent.service
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `MQTT_HOST environment variable is not set` | Missing `.env` or env var | Create `.env` in `WorkingDirectory` or add `Environment=` to service |
| `Could not connect to MQTT broker` | Wrong host/port or network issue | Verify `MQTT_HOST` is reachable: `ping $MQTT_HOST` |
| Service starts then immediately stops | Python error at startup | Check `journalctl -u node-agent.service -n 50` for traceback |
| No metrics appearing on the dashboard | Wrong `--node` ID | Ensure the ID matches what the dashboard subscribes to (`NODES` dict in `main.py`) |
