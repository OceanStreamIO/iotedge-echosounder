"""Segment-based processing pipeline orchestrator.

Called by both real-time (WebSocket) and file-based ingestion paths.
Each batch produces an independent *segment* under the day folder:

  EchoData → Sv → [denoise] → [seabed] → [MVBS] → [NASC] → [echograms]
  → segment folder with all products + metadata.json

Stages in brackets are configurable via ``EdgeConfig`` toggles.

A separate consolidation job (future) merges segments into daily
products and optionally deletes processed segments.
"""

from __future__ import annotations

import gc
import json
import logging
import time
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import numpy as np
import pandas as pd
import xarray as xr

if TYPE_CHECKING:
    from azure.iot.device import IoTHubModuleClient
    from config import EdgeConfig
    from echopype.echodata.echodata import EchoData
    from process.segment_store import SegmentStore

logger = logging.getLogger("oceanstream")


# ═══════════════════════════════════════════════════════════════════════
# Core: process one batch of EchoData → segment
# ═══════════════════════════════════════════════════════════════════════


async def process_echodata(
    echodata: "EchoData",
    config: "EdgeConfig",
    segment_store: "SegmentStore",
    client: Optional["IoTHubModuleClient"] = None,
    *,
    file_stem: Optional[str] = None,
) -> Dict[str, Any]:
    """Process a single EchoData batch and save products.

    All products land in a single campaign container (``survey_id``),
    with data-type subfolders (``processed/``, ``echograms/``).

    Two naming modes:
    - **file_stem given** (file-based ingestion): products are saved
      under ``{campaign}/processed/{file_stem}/``, echograms under
      ``{campaign}/echograms/{file_stem}/``.  No date/segment hierarchy.
    - **file_stem omitted** (real-time ingestion): products are saved
      under ``{campaign}/processed/{date}/segments/{seg}/``
      hierarchy.

    Steps:
      1. Compute Sv (with GPU if available)
      2. Save Sv
      3. Denoise (if enabled)
      4. Seabed detection (if enabled)
      5. Compute MVBS (if enabled)
      6. Compute NASC (if enabled + GPS available)
      7. Generate echograms (if enabled)
      8. Write metadata.json
      9. Send telemetry
    """
    from process.compute_sv import compute_sv
    from process.config_adapter import to_denoise_config
    from process.denoise import denoise
    from process.mvbs import compute_mvbs
    from process.nasc import compute_nasc
    from process.seabed import apply_seabed

    start_time = time.time()
    result: Dict[str, Any] = {"status": "ok"}
    storage = segment_store.storage

    # --- Step 1: Compute Sv ---
    ds_sv = compute_sv(
        echodata,
        waveform_mode=config.waveform_mode,
        encode_mode=config.encode_mode,
        use_gpu=config.use_gpu,
        depth_offset=config.depth_offset,
    )

    n_pings = ds_sv.sizes.get("ping_time", 0)
    if n_pings == 0:
        logger.warning("No valid pings after Sv computation — skipping")
        return {"status": "skipped", "reason": "no valid pings"}

    # Derive output prefix — all data lands in one campaign container
    campaign = config.campaign_container
    if file_stem:
        # File mode: flat layout under campaign container
        label = file_stem
        processed_prefix = f"{campaign}/{config.processed_container}/{file_stem}"
        echogram_prefix = f"{campaign}/{config.echogram_container}/{file_stem}"
    else:
        # Real-time mode: date/segment hierarchy
        day = segment_store.segment_day(ds_sv)
        label = segment_store.segment_name(ds_sv)
        processed_prefix = (
            f"{campaign}/{config.processed_container}/{day.isoformat()}"
            f"/segments/{label}"
        )
        echogram_prefix = (
            f"{campaign}/{config.echogram_container}/{day.isoformat()}"
            f"/segments/{label}"
        )

    result["day"] = pd.Timestamp(ds_sv["ping_time"].values[0]).date().isoformat()
    result["segment"] = label
    result["n_pings"] = int(n_pings)

    # --- Step 2: Save Sv ---
    sv_path = f"{processed_prefix}/sv.zarr"
    storage.save_zarr(ds_sv, sv_path)
    result["sv_path"] = sv_path
    _release_memory()

    # --- Step 3: Denoise ---
    ds_denoised = ds_sv
    if config.denoise_enabled:
        try:
            denoise_config = to_denoise_config(config)
            ds_denoised = denoise(ds_sv, config=denoise_config)
            storage.save_zarr(ds_denoised, f"{processed_prefix}/sv_denoised.zarr")
            result["denoise"] = "ok"
        except Exception as e:
            logger.error("Denoising failed: %s", e, exc_info=True)
            result["denoise"] = f"error: {e}"
    _release_memory()

    # --- Step 3b: Seabed ---
    if config.seabed_enabled:
        try:
            ds_denoised = apply_seabed(
                ds_denoised,
                method=config.seabed_method,
                max_range=config.seabed_max_range,
            )
            storage.save_zarr(ds_denoised, f"{processed_prefix}/sv_seabed.zarr")
            result["seabed"] = "ok"
        except Exception as e:
            logger.error("Seabed detection failed: %s", e, exc_info=True)
            result["seabed"] = f"error: {e}"
        _release_memory()

    # --- Step 4: MVBS ---
    ds_mvbs = None
    if config.mvbs_enabled:
        try:
            ds_mvbs = compute_mvbs(
                ds_denoised,
                range_bin=config.mvbs_range_bin + "m",
                ping_time_bin=config.mvbs_ping_time_bin,
            )
            if ds_mvbs.sizes:
                storage.save_zarr(ds_mvbs, f"{processed_prefix}/mvbs.zarr")
                result["mvbs"] = "ok"
        except Exception as e:
            logger.error("MVBS failed: %s", e, exc_info=True)
            result["mvbs"] = f"error: {e}"
        _release_memory()

    # --- Step 5: NASC ---
    if config.nasc_enabled:
        try:
            ds_nasc = compute_nasc(
                ds_denoised,
                range_bin=config.nasc_range_bin + "m",
                dist_bin=config.nasc_dist_bin + "nmi",
            )
            if ds_nasc.sizes:
                storage.save_zarr(ds_nasc, f"{processed_prefix}/nasc.zarr")
                result["nasc"] = "ok"
        except Exception as e:
            logger.warning("NASC failed (may need GPS): %s", e)
            result["nasc"] = f"skipped: {e}"
        _release_memory()

    # --- Step 6: Echograms ---
    if config.plot_echogram:
        try:
            from exports.echograms import generate_echograms
            echogram_items = generate_echograms(
                ds_sv=ds_sv,
                ds_denoised=ds_denoised if config.denoise_enabled else None,
                ds_mvbs=ds_mvbs,
                day=pd.Timestamp(ds_sv["ping_time"].values[0]).date(),
                config=config,
            )
            saved_paths = []
            for item in echogram_items:
                path = f"{echogram_prefix}/{item['filename']}"
                storage.save_file(item["data"], path)
                saved_paths.append(path)
            result["echogram_files"] = saved_paths
        except Exception as e:
            logger.error("Echogram generation failed: %s", e, exc_info=True)
        _release_memory()

    # --- Step 7: Metadata ---
    processing_time_ms = int((time.time() - start_time) * 1000)
    result["processing_time_ms"] = processing_time_ms

    # Extract additional scientific metadata from ds_sv
    try:
        frequencies = [float(f) for f in ds_sv["frequency_nominal"].values]
        result["frequencies_hz"] = frequencies
    except Exception:
        frequencies = []

    try:
        channels = [str(ch) for ch in ds_sv.coords["channel"].values]
        result["channels"] = channels
    except Exception:
        channels = []

    ping_times = ds_sv["ping_time"].values
    start_ts = pd.Timestamp(ping_times[0])
    end_ts = pd.Timestamp(ping_times[-1])
    result["start_time"] = start_ts.isoformat()
    result["end_time"] = end_ts.isoformat()

    depth_range = None
    for depth_var in ("echo_range", "depth"):
        if depth_var in ds_sv:
            try:
                vals = ds_sv[depth_var].values
                valid = vals[np.isfinite(vals)]
                if len(valid) > 0:
                    depth_range = [float(np.nanmin(valid)), float(np.nanmax(valid))]
                    break
            except Exception:
                pass
    if depth_range:
        result["depth_range_m"] = depth_range

    for coord in ("latitude", "longitude"):
        if coord in ds_sv:
            try:
                vals = ds_sv[coord].values
                valid = vals[np.isfinite(vals) & (vals != 0)]
                if len(valid) > 0:
                    result[f"{coord[:3]}_range"] = [float(valid.min()), float(valid.max())]
            except Exception:
                pass

    try:
        sv_vals = ds_sv["Sv"].values
        finite_mask = np.isfinite(sv_vals)
        if finite_mask.any():
            sv_mean = float(np.nanmean(sv_vals[finite_mask]))
            if np.isfinite(sv_mean):
                result["sv_mean_db"] = round(sv_mean, 1)
    except Exception:
        pass

    metadata = {
        "file": file_stem or label,
        "day": result["day"],
        "segment": label,
        "start_time": result.get("start_time"),
        "end_time": result.get("end_time"),
        "n_pings": int(n_pings),
        "n_channels": int(ds_sv.sizes.get("channel", 0)),
        "frequencies_hz": frequencies,
        "channels": channels,
        "processing_time_ms": processing_time_ms,
        "products": [k for k in ("sv", "sv_denoised", "sv_seabed", "mvbs", "nasc") if result.get(k) == "ok" or k == "sv"],
        "echogram_files": result.get("echogram_files", []),
        "config": {
            "sonar_model": config.sonar_model,
            "waveform_mode": config.waveform_mode,
            "denoise_enabled": config.denoise_enabled,
            "denoise_methods": config.denoise_methods,
            "mvbs_enabled": config.mvbs_enabled,
            "mvbs_range_bin": config.mvbs_range_bin,
            "mvbs_ping_time_bin": config.mvbs_ping_time_bin,
            "nasc_enabled": config.nasc_enabled,
            "nasc_range_bin": config.nasc_range_bin,
            "nasc_dist_bin": config.nasc_dist_bin,
            "seabed_enabled": config.seabed_enabled,
            "use_gpu": config.use_gpu,
        },
    }
    if depth_range:
        metadata["depth_range_m"] = depth_range
    if result.get("lat_range"):
        metadata["lat_range"] = result["lat_range"]
    if result.get("lon_range"):
        metadata["lon_range"] = result["lon_range"]
    if result.get("sv_mean_db") is not None:
        metadata["sv_mean_db"] = result["sv_mean_db"]

    try:
        metadata_path = f"{processed_prefix}/metadata.json"
        data = json.dumps(metadata, indent=2, default=str).encode("utf-8")
        storage.save_file(data, metadata_path)
        logger.info("Saved metadata → %s", metadata_path)
        # Also write to local filesystem for the edgeai RAG indexer
        _write_local_copy(metadata_path, data)
    except Exception as e:
        logger.error("Failed to save metadata: %s", e)

    # --- Step 7b: Segment Markdown Report ---
    try:
        from exports.report_md import generate_segment_report
        report_md = generate_segment_report(result, metadata, config)
        report_path = f"{processed_prefix}/segment_report.md"
        report_data = report_md.encode("utf-8")
        storage.save_file(report_data, report_path)
        logger.info("Saved segment report → %s", report_path)
        _write_local_copy(report_path, report_data)
    except Exception as e:
        logger.error("Failed to save segment report: %s", e)

    # --- Step 8: Telemetry ---
    if client:
        try:
            from exports.telemetry import send_processing_telemetry
            send_processing_telemetry(client, result, config)
        except Exception as e:
            logger.error("Telemetry send failed: %s", e)

    logger.info(
        "%s complete: %d pings in %dms",
        label, n_pings, processing_time_ms,
    )
    return result


# ═══════════════════════════════════════════════════════════════════════
# File trigger: raw file → full pipeline
# ═══════════════════════════════════════════════════════════════════════


async def process_raw_file_pipeline(
    file_path: str,
    config: "EdgeConfig",
    segment_store: "SegmentStore",
    client: Optional["IoTHubModuleClient"] = None,
) -> Dict[str, Any]:
    """Full pipeline for a single raw file.

    Converts raw → EchoData → saves to ``{campaign}/echodata/{stem}.zarr`` →
    delegates to ``process_echodata()`` with ``file_stem`` so all
    products land in the campaign container using the file stem as
    the sub-folder key.

    Expected layout (campaign container = survey_id):
    ::

        {campaign}/echodata/{stem}.zarr
        {campaign}/processed/{stem}/sv.zarr
        {campaign}/processed/{stem}/sv_denoised.zarr
        {campaign}/processed/{stem}/mvbs.zarr
        {campaign}/processed/{stem}/metadata.json
        {campaign}/echograms/{stem}/sv_38kHz.png
    """
    from process.convert import convert_raw_file, save_echodata_zarr

    start = time.time()
    stem = Path(file_path).stem
    logger.info("File pipeline: %s  (stem=%s)", file_path, stem)

    echodata = convert_raw_file(file_path, sonar_model=config.sonar_model)

    # Set platform metadata from config
    try:
        echodata["Platform"].attrs["platform_type"] = config.platform_type
        echodata["Platform"].attrs["platform_name"] = config.platform_name
        echodata["Platform"].attrs["platform_code_ICES"] = config.platform_code_ICES
        echodata["Top-level"].attrs["title"] = (
            f"{config.survey_name} [{config.survey_id}], file {stem}"
        )
    except Exception as e:
        logger.debug("Could not set platform metadata: %s", e)

    # Save converted EchoData → {campaign}/echodata/{stem}.zarr
    # EchoData is a DataTree (multiple groups); it cannot round-trip
    # through xr.open_zarr() which only reads the root group.
    campaign = config.campaign_container
    echodata_storage_path = f"{campaign}/{config.converted_container}/{stem}.zarr"
    try:
        segment_store.storage.save_echodata(echodata, echodata_storage_path)
        logger.info("Saved EchoData → %s", echodata_storage_path)
    except Exception as e:
        logger.warning("Failed to save EchoData to storage: %s", e)
        echodata_storage_path = ""

    result = await process_echodata(
        echodata, config, segment_store, client, file_stem=stem,
    )
    result["source_file"] = Path(file_path).name
    result["echodata_path"] = echodata_storage_path
    result["total_time_ms"] = int((time.time() - start) * 1000)

    # Send ML payload (preserves existing outputml route)
    if client and result.get("sv_path"):
        try:
            from azure_handler.message_handler import send_to_hub
            ml_payload = {
                "file_name": Path(file_path).name,
                "sv_zarr_path": result["sv_path"],
                "campaign_id": config.survey_id,
                "dataset_id": stem,
                "depth_offset": config.depth_offset,
                "date": result.get("day", ""),
            }
            send_to_hub(client, ml_payload, output_name="outputml")
        except Exception as e:
            logger.error("ML payload send failed: %s", e)

    return result


_LOCAL_BASE = Path("/app/processed")


def _write_local_copy(blob_path: str, data: bytes) -> None:
    """Write a copy to the local filesystem for the edgeai RAG indexer.

    The blob_path looks like ``{campaign}/processed/2023-06-25/segments/.../file``.
    Strip the leading campaign container name to get the relative
    path, then write under ``/app/processed/`` which is bind-mounted to
    ``/data/processed/`` on the host — where the edgeai file watcher runs.
    """
    try:
        parts = blob_path.split("/", 1)
        rel = parts[1] if len(parts) > 1 else parts[0]
        dest = _LOCAL_BASE / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        logger.debug("Local copy → %s", dest)
    except Exception as e:
        logger.warning("Failed to write local copy of %s: %s", blob_path, e)


def _release_memory() -> None:
    """Free memory between pipeline stages — critical on 16 GB Jetson."""
    gc.collect()
    try:
        import torch
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
    except ImportError:
        pass
    try:
        import cupy as cp
        pool = cp.get_default_memory_pool()
        pool.free_all_blocks()
    except (ImportError, Exception):
        pass
