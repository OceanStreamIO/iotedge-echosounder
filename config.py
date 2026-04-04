"""Edge processing configuration.

Populated from IoT Hub module twin desired properties and environment
variables.  Provides a single typed dataclass for all processing
parameters.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, fields
from typing import Any, Dict, Literal, Optional

logger = logging.getLogger("oceanstream")


def _parse_bool(val: Any, default: bool = True) -> bool:
    """Parse a value that might be bool, str, int, or None into a Python bool.

    Handles IoT Hub twin quirks where booleans arrive as ``"false"`` strings.
    """
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        return val.lower() not in ("false", "0", "no", "off", "")
    return default


def _unwrap(val: Any) -> Any:
    """Unwrap ``{"value": X}`` IoT Hub twin wrapper, if present."""
    if isinstance(val, dict) and "value" in val:
        return val["value"]
    return val


# Twin key → dataclass field name (for keys that differ from field names)
_TWIN_KEY_MAP: dict[str, str] = {"Log_Level": "log_level"}

# Fields whose values need _parse_bool
_BOOL_FIELDS: set[str] = {
    "use_gpu", "denoise_enabled", "mvbs_enabled", "nasc_enabled",
    "plot_echogram", "seabed_enabled", "denoise_use_frequency_specific",
}

# Fields whose values need int()
_INT_FIELDS: set[str] = {"realtime_buffer_seconds", "realtime_buffer_pings", "realtime_min_pings"}

# Fields whose values need float()
_FLOAT_FIELDS: set[str] = {"depth_offset", "seabed_max_range"}


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

    # --- Real-time EK80 service ---
    ek80_service_url: str = "http://localhost:8050"
    realtime_buffer_seconds: int = 1800     # Time-based trigger (30 min)
    realtime_buffer_pings: int = 100        # Ping-count trigger
    realtime_min_pings: int = 50            # Minimum pings for any batch

    # --- GPU ---
    use_gpu: bool = True

    # --- Pipeline toggles ---
    denoise_enabled: bool = True
    mvbs_enabled: bool = True
    nasc_enabled: bool = False
    plot_echogram: bool = True

    # --- Denoise ---
    denoise_methods: str = "background,transient,impulse,attenuation"
    denoise_use_frequency_specific: bool = False

    # --- Seabed ---
    seabed_enabled: bool = False
    seabed_method: str = "ariza"
    seabed_max_range: float = 1000.0

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
            return _unwrap(val)

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
            ek80_service_url=os.getenv("EK80_SERVICE_URL", _get("ek80_service_url", "http://localhost:8050")),
            realtime_buffer_seconds=int(_get("realtime_buffer_seconds", 1800)),
            realtime_buffer_pings=int(_get("realtime_buffer_pings", 100)),
            realtime_min_pings=int(_get("realtime_min_pings", 50)),
            use_gpu=_parse_bool(_get("use_gpu", True)),
            denoise_enabled=_parse_bool(_get("denoise_enabled", True)),
            mvbs_enabled=_parse_bool(_get("mvbs_enabled", True)),
            nasc_enabled=_parse_bool(_get("nasc_enabled", False), default=False),
            plot_echogram=_parse_bool(_get("plot_echogram", True)),
            denoise_methods=str(_get("denoise_methods", "background,transient,impulse,attenuation")),
            denoise_use_frequency_specific=_parse_bool(_get("denoise_use_frequency_specific", False), default=False),
            seabed_enabled=_parse_bool(_get("seabed_enabled", False), default=False),
            seabed_method=str(_get("seabed_method", "ariza")),
            seabed_max_range=float(_get("seabed_max_range", 1000.0)),
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
        """Apply a twin desired-properties patch in place.

        Only updates fields that are present in the patch.  Unknown keys
        (e.g. ``$version``) are ignored.  This avoids resetting fields
        that were not included in a partial patch.
        """
        known_fields = {f.name for f in fields(self)}

        for key, raw_val in patch.items():
            val = _unwrap(raw_val)
            # Map twin key to field name (e.g. "Log_Level" → "log_level")
            field_name = _TWIN_KEY_MAP.get(key, key)
            if field_name not in known_fields:
                continue  # skip $version, unknown keys

            if field_name in _BOOL_FIELDS:
                setattr(self, field_name, _parse_bool(val))
            elif field_name in _INT_FIELDS:
                try:
                    setattr(self, field_name, int(val))
                except (ValueError, TypeError):
                    pass
            elif field_name in _FLOAT_FIELDS:
                try:
                    setattr(self, field_name, float(val))
                except (ValueError, TypeError):
                    pass
            else:
                setattr(self, field_name, str(val) if not isinstance(val, str) else val)

        logger.info("Config updated from twin patch: %s", [k for k in patch if not k.startswith("$")])
