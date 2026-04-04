"""File-based trigger processing for IoT Edge messages.

Handles ``rawfileadded`` input messages from the filenotifier module
and runs the full processing pipeline on each raw file.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from azure.iot.device import IoTHubModuleClient
    from config import EdgeConfig
    from process.segment_store import SegmentStore

logger = logging.getLogger("oceanstream")


async def handle_raw_file_added(
    message_data: Dict[str, Any],
    config: "EdgeConfig",
    segment_store: "SegmentStore",
    client: "IoTHubModuleClient",
) -> Dict[str, Any]:
    """Process a raw file triggered by an IoT Edge message.

    Parameters
    ----------
    message_data
        Decoded JSON from the ``rawfileadded`` input message.
        Expected keys: ``event``, ``file_added_path``.
    config
        Current edge configuration.
    segment_store
        Segment-based Zarr store manager.
    client
        IoT Hub module client for sending results.

    Returns
    -------
    dict
        Processing result summary.
    """
    from process.pipeline import process_raw_file_pipeline

    event = message_data.get("event")
    file_path = message_data.get("file_added_path", "")

    if event != "fileadd":
        logger.info("Ignoring non-fileadd event: %s", event)
        return {"status": "skipped", "reason": f"event={event}"}

    if not file_path or not Path(file_path).exists():
        logger.error("Raw file not found: %s", file_path)
        return {"status": "error", "reason": f"file not found: {file_path}"}

    logger.info("Processing raw file: %s", file_path)
    result = await process_raw_file_pipeline(
        file_path=file_path,
        config=config,
        segment_store=segment_store,
        client=client,
    )

    return result


def parse_input_message(message) -> Dict[str, Any]:
    """Parse an IoT Edge input message to a dict.

    Parameters
    ----------
    message
        IoT Hub Message object with ``.data`` bytes payload.

    Returns
    -------
    dict
        Parsed JSON content.
    """
    try:
        byte_str = message.data
        return json.loads(byte_str.decode("utf-8"))
    except Exception as e:
        logger.error("Failed to parse input message: %s", e)
        return {}
