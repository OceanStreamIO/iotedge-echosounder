# Imports that don't require Azure SDK
from .plot import plot_sv_data, plot_individual_channel_simplified
from .metadata import create_instrument_metadata
from .location import extract_location_data, select_location_points, create_location_message
from .pdf import generate_processing_report
from .echograms import generate_echograms
from .report_md import generate_md_report


def __getattr__(name: str):
    """Lazy-load symbols that depend on Azure SDK."""
    if name == "send_to_iot_hub" or name == "plot_and_upload_echograms":
        from . import export_handler
        return getattr(export_handler, name)
    if name == "send_processing_telemetry":
        from . import telemetry
        return getattr(telemetry, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "plot_sv_data",
    "plot_individual_channel_simplified",
    "create_instrument_metadata",
    "extract_location_data",
    "select_location_points",
    "create_location_message",
    "send_to_iot_hub",
    "plot_and_upload_echograms",
    "generate_processing_report",
    "generate_echograms",
    "generate_md_report",
    "send_processing_telemetry",
]
