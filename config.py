"""Edge processing configuration.

Populated from IoT Hub module twin desired properties and environment
variables.  Provides a single typed dataclass for all processing
parameters.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional

logger = logging.getLogger("oceanstream")


@dataclass
class EdgeConfig:
    """Unified configuration for the echosounder edge module."""

    # --- Sonar / calibration ---
    sonar_model: str = "EK80"
    waveform_mode: str = "CW"
    encode_mode: str = "power"
    depth_offset: float = 0.0

    # --- Survey / platform metadata ---
    survey_id: str = ""
    survey_name: str = ""
    platform_type: str = ""
    platform_name: str = ""
    platform_code_ICES: str = ""

    # --- Processing mode ---
    processing_mode: Literal["realtime", "file", "both"] = "both"

    # --- Real-time UDP settings ---
    ek80_host: str = "192.168.0.105"
    ek80_port: int = 37655
    realtime_buffer_seconds: int = 60

    # --- GPU ---
    use_gpu: bool = True

    # --- Pipeline toggles ---
    denoise_enabled: bool = True
    plot_echogram: bool = True

    # --- MVBS bins ---
    mvbs_range_bin: str = "0.5"
    mvbs_ping_time_bin: str = "10s"

    # --- NASC bins ---
    nasc_range_bin: str = "10"
    nasc_dist_bin: str = "0.5"

    # --- Storage ---
    storage_backend: Literal["azure-blob-edge", "minio", "local"] = "azure-blob-edge"
    output_base_path: str = "/app/processed"
    converted_container: str = "converted"
    echogram_container: str = "echograms"
    processed_container: str = "processed"
    pdf_output_path: str = "/app/pdf_output"

    # --- Logging ---
    log_level: str = "INFO"

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_twin_and_env(
        cls,
        twin_desired: Dict[str, Any],
    ) -> "EdgeConfig":
        """Create config from IoT Hub module twin + environment variables.

        Twin property values may be plain scalars or ``{"value": ...}``
        dicts (IoT Edge convention).
        """

        def _get(key: str, default: Any = "") -> Any:
            val = twin_desired.get(key, default)
            # Unwrap {"value": X} wrapper used by IoT Hub twin
            if isinstance(val, dict) and "value" in val:
                val = val["value"]
            return val

        depth_offset_raw = _get("depth_offset", 0)
        if isinstance(depth_offset_raw, str):
            depth_offset_raw = float(depth_offset_raw) if depth_offset_raw else 0.0

        return cls(
            sonar_model=_get("sonar_model", "EK80"),
            waveform_mode=_get("waveform_mode", "CW"),
            encode_mode=_get("encode_mode", "power"),
            depth_offset=float(depth_offset_raw),
            survey_id=_get("survey_id", ""),
            survey_name=_get("survey_name", ""),
            platform_type=_get("platform_type", ""),
            platform_name=_get("platform_name", ""),
            platform_code_ICES=_get("platform_code_ICES", ""),
            processing_mode=_get("processing_mode", "both"),
            ek80_host=os.getenv("EK80_HOST", _get("ek80_host", "192.168.0.105")),
            ek80_port=int(os.getenv("EK80_PORT", _get("ek80_port", 37655))),
            realtime_buffer_seconds=int(_get("realtime_buffer_seconds", 60)),
            use_gpu=bool(_get("use_gpu", True)),
            denoise_enabled=bool(_get("denoise_enabled", True)),
            plot_echogram=bool(_get("plot_echogram", True)),
            mvbs_range_bin=str(_get("mvbs_range_bin", "0.5")),
            mvbs_ping_time_bin=str(_get("mvbs_ping_time_bin", "10s")),
            nasc_range_bin=str(_get("nasc_range_bin", "10")),
            nasc_dist_bin=str(_get("nasc_dist_bin", "0.5")),
            storage_backend=os.getenv("STORAGE_BACKEND", _get("storage_backend", "azure-blob-edge")),
            output_base_path=os.getenv("OUTPUT_BASE_PATH", "/app/processed"),
            converted_container=os.getenv("CONVERTED_CONTAINER_NAME", "converted"),
            echogram_container=os.getenv("ECHOGRAM_CONTAINER_NAME", "echograms"),
            processed_container=os.getenv("PROCESSED_CONTAINER_NAME", "processed"),
            pdf_output_path=os.getenv("PDF_OUTPUT_PATH", "/app/pdf_output"),
            log_level=_get("Log_Level", "INFO"),
        )

    def update_from_twin(self, patch: Dict[str, Any]) -> None:
        """Apply a twin desired-properties patch in place."""
        new = self.from_twin_and_env(patch)
        for f in self.__dataclass_fields__:
            setattr(self, f, getattr(new, f))
