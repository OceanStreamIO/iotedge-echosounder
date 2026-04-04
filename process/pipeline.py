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
) -> Dict[str, Any]:
    """Process a single EchoData batch and save as an independent segment.

    Steps:
      1. Compute Sv (with GPU if available)
      2. Save Sv to segment
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

    # Derive segment identity
    day = segment_store.segment_day(ds_sv)
    seg_name = segment_store.segment_name(ds_sv)

    result["day"] = day.isoformat()
    result["segment"] = seg_name
    result["n_pings"] = int(n_pings)

    # --- Step 2: Save Sv ---
    sv_path = segment_store.save_zarr(ds_sv, day, seg_name, "sv")
    result["sv_path"] = sv_path
    _release_memory()

    # --- Step 3: Denoise ---
    ds_denoised = ds_sv
    if config.denoise_enabled:
        try:
            denoise_config = to_denoise_config(config)
            ds_denoised = denoise(ds_sv, config=denoise_config)
            segment_store.save_zarr(ds_denoised, day, seg_name, "sv_denoised")
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
            segment_store.save_zarr(ds_denoised, day, seg_name, "sv_seabed")
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
                segment_store.save_zarr(ds_mvbs, day, seg_name, "mvbs")
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
                segment_store.save_zarr(ds_nasc, day, seg_name, "nasc")
                result["nasc"] = "ok"
        except Exception as e:
            logger.warning("NASC failed (may need GPS): %s", e)
            result["nasc"] = f"skipped: {e}"
        _release_memory()

    # --- Step 6: Echograms ---
    if config.plot_echogram:
        try:
            from exports.echograms import generate_echograms
            echogram_files = generate_echograms(
                ds_sv=ds_sv,
                ds_denoised=ds_denoised if config.denoise_enabled else None,
                ds_mvbs=ds_mvbs,
                day=day,
                config=config,
                output_subdir=f"segments/{seg_name}/echograms",
            )
            result["echogram_files"] = echogram_files
        except Exception as e:
            logger.error("Echogram generation failed: %s", e, exc_info=True)
        _release_memory()

    # --- Step 7: Metadata ---
    processing_time_ms = int((time.time() - start_time) * 1000)
    result["processing_time_ms"] = processing_time_ms

    metadata = {
        "segment": seg_name,
        "day": day.isoformat(),
        "n_pings": int(n_pings),
        "n_channels": int(ds_sv.sizes.get("channel", 0)),
        "processing_time_ms": processing_time_ms,
        "products": [k for k in ("sv", "sv_denoised", "sv_seabed", "mvbs", "nasc") if result.get(k) == "ok" or k == "sv"],
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
    try:
        segment_store.save_metadata(metadata, day, seg_name)
    except Exception as e:
        logger.error("Failed to save segment metadata: %s", e)

    # --- Step 8: Telemetry ---
    if client:
        try:
            from exports.telemetry import send_processing_telemetry
            send_processing_telemetry(client, result, config)
        except Exception as e:
            logger.error("Telemetry send failed: %s", e)

    logger.info(
        "Segment %s complete: %d pings in %dms",
        seg_name, n_pings, processing_time_ms,
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

    Converts raw → EchoData → delegates to ``process_echodata()``.
    """
    from process.convert import convert_raw_file

    start = time.time()
    logger.info("File pipeline: %s", file_path)

    echodata = convert_raw_file(file_path, sonar_model=config.sonar_model)

    # Set platform metadata from config
    try:
        echodata["Platform"].attrs["platform_type"] = config.platform_type
        echodata["Platform"].attrs["platform_name"] = config.platform_name
        echodata["Platform"].attrs["platform_code_ICES"] = config.platform_code_ICES
        echodata["Top-level"].attrs["title"] = (
            f"{config.survey_name} [{config.survey_id}], file {Path(file_path).stem}"
        )
    except Exception as e:
        logger.debug("Could not set platform metadata: %s", e)

    result = await process_echodata(echodata, config, segment_store, client)
    result["source_file"] = Path(file_path).name
    result["total_time_ms"] = int((time.time() - start) * 1000)

    # Send ML payload (preserves existing outputml route)
    if client and result.get("sv_path"):
        try:
            from azure_handler.message_handler import send_to_hub
            ml_payload = {
                "file_name": Path(file_path).name,
                "sv_zarr_path": result["sv_path"],
                "campaign_id": config.survey_id,
                "dataset_id": Path(file_path).stem,
                "depth_offset": config.depth_offset,
                "date": result.get("day", ""),
            }
            send_to_hub(client, ml_payload, output_name="outputml")
        except Exception as e:
            logger.error("ML payload send failed: %s", e)

    return result


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
