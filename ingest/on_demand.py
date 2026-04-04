"""On-demand processing via Cloud-to-Device (C2D) messages.

Supports commands:
- ``process_raw_files``: process specific raw files
- ``process_day``: reprocess all data for a given date
- ``get_status``: return current processing state
- ``set_config``: update processing parameters at runtime
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from azure.iot.device import IoTHubModuleClient
    from config import EdgeConfig
    from process.segment_store import SegmentStore

logger = logging.getLogger("oceanstream")


async def handle_c2d_command(
    message_data: Dict[str, Any],
    config: "EdgeConfig",
    segment_store: "SegmentStore",
    client: "IoTHubModuleClient",
    job_queue: asyncio.Queue,
) -> Dict[str, Any]:
    """Dispatch a C2D command message.

    Parameters
    ----------
    message_data
        Parsed JSON with ``command`` key and command-specific payload.
    config
        Current edge configuration.
    segment_store
        Segment-based Zarr store manager.
    client
        IoT Hub module client.
    job_queue
        Async queue for long-running jobs to prevent concurrent heavy ops.

    Returns
    -------
    dict
        Immediate ack with job_id or result.
    """
    command = message_data.get("command", "")

    if command == "process_raw_files":
        return await _cmd_process_raw_files(message_data, config, segment_store, client, job_queue)
    elif command == "process_day":
        return await _cmd_process_day(message_data, config, segment_store, client, job_queue)
    elif command == "get_status":
        return _cmd_get_status(config, segment_store, job_queue)
    elif command == "set_config":
        return _cmd_set_config(message_data, config)
    else:
        logger.warning("Unknown C2D command: %s", command)
        return {"status": "error", "reason": f"unknown command: {command}"}


async def _cmd_process_raw_files(
    data: Dict[str, Any],
    config: "EdgeConfig",
    segment_store: "SegmentStore",
    client: "IoTHubModuleClient",
    job_queue: asyncio.Queue,
) -> Dict[str, Any]:
    """Queue processing of specific raw files."""
    paths = data.get("paths", [])
    options = data.get("options", {})

    if not paths:
        return {"status": "error", "reason": "no paths specified"}

    import uuid
    job_id = str(uuid.uuid4())[:8]

    # Queue each file as a job
    for path in paths:
        await job_queue.put({
            "type": "process_raw",
            "job_id": job_id,
            "file_path": path,
            "options": options,
        })

    logger.info("Queued %d raw files for processing (job=%s)", len(paths), job_id)
    return {"status": "accepted", "job_id": job_id, "files_queued": len(paths)}


async def _cmd_process_day(
    data: Dict[str, Any],
    config: "EdgeConfig",
    segment_store: "SegmentStore",
    client: "IoTHubModuleClient",
    job_queue: asyncio.Queue,
) -> Dict[str, Any]:
    """Queue reprocessing of a day's data."""
    date_str = data.get("date", "")
    stages = data.get("stages", ["denoise", "mvbs", "nasc", "echograms"])

    try:
        target_date = date.fromisoformat(date_str)
    except (ValueError, TypeError):
        return {"status": "error", "reason": f"invalid date: {date_str}"}

    segments = segment_store.list_segments(target_date)
    if not segments:
        return {"status": "error", "reason": f"no segments for {date_str}"}

    import uuid
    job_id = str(uuid.uuid4())[:8]

    await job_queue.put({
        "type": "process_day",
        "job_id": job_id,
        "date": target_date,
        "stages": stages,
    })

    logger.info("Queued day reprocessing for %s (job=%s, stages=%s)", date_str, job_id, stages)
    return {"status": "accepted", "job_id": job_id, "date": date_str}


def _cmd_get_status(
    config: "EdgeConfig",
    segment_store: "SegmentStore",
    job_queue: asyncio.Queue,
) -> Dict[str, Any]:
    """Return current processing status."""
    import psutil

    mem = psutil.virtual_memory()

    # Check GPU status
    gpu_available = False
    gpu_mem_used = 0
    gpu_mem_total = 0
    try:
        from echopype.utils.gpu import has_cuda
        gpu_available = has_cuda()
        if gpu_available:
            import cupy as cp
            mem_info = cp.cuda.Device(0).mem_info
            gpu_mem_used = mem_info[1] - mem_info[0]
            gpu_mem_total = mem_info[1]
    except Exception:
        pass

    days = segment_store.list_days()

    return {
        "status": "ok",
        "processing_mode": config.processing_mode,
        "gpu_available": gpu_available,
        "gpu_memory_used_mb": gpu_mem_used // (1024 * 1024),
        "gpu_memory_total_mb": gpu_mem_total // (1024 * 1024),
        "system_memory_used_mb": mem.used // (1024 * 1024),
        "system_memory_total_mb": mem.total // (1024 * 1024),
        "system_memory_percent": mem.percent,
        "jobs_pending": job_queue.qsize(),
        "days_with_data": [d.isoformat() for d in days],
        "sonar_model": config.sonar_model,
        "storage_backend": config.storage_backend,
    }


def _cmd_set_config(
    data: Dict[str, Any],
    config: "EdgeConfig",
) -> Dict[str, Any]:
    """Update configuration at runtime."""
    new_config = data.get("config", {})
    if not new_config:
        return {"status": "error", "reason": "no config provided"}

    config.update_from_twin(new_config)
    logger.info("Config updated: %s", new_config)
    return {"status": "ok", "updated_keys": list(new_config.keys())}


async def job_worker(
    job_queue: asyncio.Queue,
    config: "EdgeConfig",
    segment_store: "SegmentStore",
    client: "IoTHubModuleClient",
) -> None:
    """Background worker that processes jobs from the queue sequentially.

    This ensures only one heavy processing job runs at a time on the
    Jetson's limited 16 GB memory.
    """
    from azure_handler.message_handler import send_to_hub
    from process.pipeline import process_raw_file_pipeline

    while True:
        job = await job_queue.get()
        try:
            job_type = job.get("type")
            job_id = job.get("job_id", "?")

            if job_type == "process_raw":
                file_path = job["file_path"]
                logger.info("Job %s: processing raw file %s", job_id, file_path)
                result = await process_raw_file_pipeline(
                    file_path=file_path,
                    config=config,
                    segment_store=segment_store,
                    client=client,
                )
                result["job_id"] = job_id
                send_to_hub(client, data=result, output_name="output1")

            elif job_type == "process_day":
                target_date = job["date"]
                stages = job["stages"]
                logger.info("Job %s: reprocessing %s stages=%s", job_id, target_date, stages)
                # Segment-based: reprocessing individual segments is a future feature
                result = {"status": "not_implemented", "reason": "segment-based reprocessing pending"}
                result["job_id"] = job_id
                send_to_hub(client, data=result, output_name="output1")

            else:
                logger.warning("Unknown job type: %s", job_type)

        except Exception as e:
            logger.error("Job %s failed: %s", job.get("job_id", "?"), e, exc_info=True)
        finally:
            job_queue.task_done()
