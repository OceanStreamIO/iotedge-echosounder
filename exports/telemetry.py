"""Send processing telemetry and results to IoT Hub."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from azure.iot.device import IoTHubModuleClient
    from config import EdgeConfig

logger = logging.getLogger("oceanstream")


def send_processing_telemetry(
    client: "IoTHubModuleClient",
    result: Dict[str, Any],
    config: "EdgeConfig",
) -> None:
    """Send processing result telemetry via IoT Hub output message.

    Also patches module twin reported properties with processing stats.
    """
    from azure_handler.message_handler import send_to_hub

    # Augment result with context
    payload = {
        **result,
        "campaign_id": config.survey_id,
        "sonar_model": config.sonar_model,
        "processing_mode": config.processing_mode,
        "use_gpu": config.use_gpu,
    }

    # Send as output message
    send_to_hub(client, data=payload, output_name="output1")

    # Update reported twin properties with latest processing stats
    try:
        reported = {
            "last_processed_day": result.get("day", ""),
            "last_processing_time_ms": result.get("processing_time_ms", 0),
            "last_n_pings": result.get("n_pings", 0),
            "processing_mode": config.processing_mode,
            "gpu_enabled": config.use_gpu,
        }
        client.patch_twin_reported_properties(reported)
    except Exception as e:
        logger.error("Failed to update reported properties: %s", e)
