"""IoT Edge echosounder module — entry point.

Dual-mode processing:
- **realtime**: Connects to EK80 via UDP, subscribes to SampleData,
  buffers pings, and processes through the full pipeline (Sv → denoise
  → MVBS → NASC → echograms) with GPU acceleration.
- **file**: Receives ``rawfileadded`` messages from the filenotifier
  module and processes raw files through the same pipeline.
- **both**: Both modes run concurrently.

On-demand C2D commands are always available for manual triggers and
status queries.

Configuration comes from:
- IoT Hub module twin desired properties
- Environment variables (see Dockerfile and streambase.config.json)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger("oceanstream")

# Quiet noisy libraries
for _noisy in ("azure", "adlfs", "distributed", "dask", "fsspec", "zarr", "urllib3"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)


async def async_main() -> None:
    """Async entry point — sets up IoT Hub client, config, and mode dispatcher."""
    from azure_handler import create_client, create_storage
    from config import EdgeConfig
    from ingest.file_trigger import handle_raw_file_added, parse_input_message
    from ingest.on_demand import handle_c2d_command, job_worker
    from ingest.realtime import RealtimeIngestion
    from process.segment_store import SegmentStore
    from process.pipeline import process_echodata

    # --- IoT Hub client ---
    client = create_client()
    logger.info("IoT Hub module client initialized")

    # --- Configuration from twin ---
    twin = client.get_twin()
    desired = twin.get("desired", {})
    config = EdgeConfig.from_twin_and_env(desired)
    logging.getLogger("oceanstream").setLevel(getattr(logging, config.log_level.upper(), logging.INFO))
    logger.info("Config loaded: mode=%s, sonar=%s, gpu=%s", config.processing_mode, config.sonar_model, config.use_gpu)

    # Log GPU status
    try:
        from echopype.utils.gpu import has_cuda
        logger.info("CUDA available: %s", has_cuda())
    except Exception:
        logger.info("CUDA check failed — GPU disabled")

    # --- Storage backend ---
    storage = create_storage(
        backend=config.storage_backend,
        base_path=config.output_base_path,
    )
    segment_store = SegmentStore(
        storage,
        container=config.campaign_container,
        processed_subfolder=config.processed_container,
    )
    logger.info(
        "Storage backend: %s, campaign container: %s",
        config.storage_backend, config.campaign_container,
    )

    # --- Job queue for on-demand processing ---
    job_queue: asyncio.Queue = asyncio.Queue(maxsize=100)

    # Capture the running loop for thread-safe callback scheduling.
    # Azure IoT SDK callbacks run on SDK worker threads, not the asyncio
    # event loop thread — we must use call_soon_threadsafe.
    loop = asyncio.get_running_loop()

    # --- Twin update handler ---
    def on_twin_update(patch):
        nonlocal config
        logger.info("Twin patch received: %s", list(patch.keys()))
        config.update_from_twin(patch)
        try:
            client.patch_twin_reported_properties(patch)
        except Exception as e:
            logger.error("Failed to report twin: %s", e)

    client.on_twin_desired_properties_patch_received = on_twin_update

    # --- File trigger handler (rawfileadded input) ---
    def on_message(message):
        if message.input_name == "rawfileadded":
            data = parse_input_message(message)
            if data.get("event") == "fileadd":
                loop.call_soon_threadsafe(
                    loop.create_task,
                    handle_raw_file_added(data, config, segment_store, client),
                )
        elif message.input_name == "c2d":
            data = parse_input_message(message)
            loop.call_soon_threadsafe(
                loop.create_task,
                handle_c2d_command(data, config, segment_store, client, job_queue),
            )

    client.on_message_received = on_message

    # --- Start background tasks ---
    tasks = []

    # Job worker (always active)
    tasks.append(asyncio.create_task(job_worker(job_queue, config, segment_store, client)))

    # Real-time ingestion (if enabled)
    realtime: RealtimeIngestion | None = None
    if config.processing_mode in ("realtime", "both"):
        async def on_batch(echodata, cfg):
            await process_echodata(echodata, cfg, segment_store, client)

        realtime = RealtimeIngestion(config, on_batch=on_batch)
        await realtime.start()

    logger.info("Module started — mode=%s, waiting for events...", config.processing_mode)

    # --- Run until terminated ---
    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Shutdown signal received")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    await stop_event.wait()

    # --- Graceful shutdown ---
    logger.info("Shutting down...")
    if realtime:
        await realtime.stop()

    for task in tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    client.shutdown()
    logger.info("Module stopped")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
