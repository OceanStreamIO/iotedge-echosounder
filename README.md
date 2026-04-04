# iotedge-echosounder

Azure IoT Edge module for echosounder data processing. Supports two ingestion modes:

- **realtime** — Connects to the EK80 service (`ek80-service` on port 8050) via WebSocket, buffers pings, and processes through the full pipeline (Sv → denoise → MVBS → NASC → echograms).
- **file** — Receives `rawfileadded` messages from the filenotifier module and processes raw `.raw` files through the same pipeline.
- **both** — Both modes run concurrently (default).

On-demand C2D commands are always available for manual triggers and status queries.

## Setup

```bash
pip install -r requirements.txt
```

## Usage

The module is designed to run as an IoT Edge module (via Docker), started by:

```bash
python -u main.py
```

Configuration comes from IoT Hub module twin desired properties and environment variables (see `config.py`).
