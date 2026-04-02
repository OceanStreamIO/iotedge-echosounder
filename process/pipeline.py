"""Unified processing pipeline orchestrator.

Called by both real-time (UDP) and file-based ingestion paths.
Implements the full edge processing flow:

  EchoData → Sv (GPU) → denoise → append day store → MVBS → NASC → echograms

Also provides ``process_raw_file_pipeline()`` for file triggers and
``process_day_pipeline()`` for on-demand reprocessing.
"""

from __future__ import annotations

import gc
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
    from process.day_store import DayStore

logger = logging.getLogger("oceanstream")


# ═══════════════════════════════════════════════════════════════════════
# Core: process one batch of EchoData
# ═══════════════════════════════════════════════════════════════════════


async def process_echodata(
    echodata: "EchoData",
    config: "EdgeConfig",
    day_store: "DayStore",
    client: Optional["IoTHubModuleClient"] = None,
) -> Dict[str, Any]:
    """Process a single EchoData batch through the full pipeline.

    Steps:
      1. Compute Sv (with GPU if available)
      2. Append to daily Zarr store
      3. Denoise (if enabled)
      4. Compute MVBS
      5. Compute NASC
      6. Generate echograms
      7. Send telemetry

    Parameters
    ----------
    echodata
        Converted EchoData (from file or PingAccumulator).
    config
        Edge processing configuration.
    day_store
        Daily Zarr store manager.
    client
        IoT Hub module client for sending results (optional).

    Returns
    -------
    dict
        Processing result metadata.
    """
    from process.compute_sv import compute_sv
    from process.denoise import denoise
    from process.mvbs import compute_mvbs
    from process.nasc import compute_nasc

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

    # Determine the day
    first_time = ds_sv["ping_time"].values[0]
    day = pd.Timestamp(first_time).date()

    result["day"] = day.isoformat()
    result["n_pings"] = int(n_pings)

    # --- Step 2: Append to daily store ---
    sv_path = day_store.append_sv(ds_sv, day)
    result["sv_path"] = sv_path
    _release_memory()

    # --- Step 3: Denoise ---
    ds_denoised = ds_sv  # fallback if denoising disabled
    if config.denoise_enabled:
        try:
            ds_denoised = denoise(ds_sv, use_gpu=config.use_gpu)
            day_store.save_product(ds_denoised, day, "sv_denoised")
        except Exception as e:
            logger.error("Denoising failed: %s", e, exc_info=True)
    _release_memory()

    # --- Step 4: MVBS ---
    try:
        ds_mvbs = compute_mvbs(
            ds_denoised,
            range_bin=config.mvbs_range_bin + "m",
            ping_time_bin=config.mvbs_ping_time_bin,
        )
        if ds_mvbs.sizes:
            day_store.save_product(ds_mvbs, day, "mvbs")
            result["mvbs_computed"] = True
    except Exception as e:
        logger.error("MVBS failed: %s", e, exc_info=True)
        result["mvbs_computed"] = False
    _release_memory()

    # --- Step 5: NASC ---
    try:
        ds_nasc = compute_nasc(
            ds_denoised,
            range_bin=config.nasc_range_bin + "m",
            dist_bin=config.nasc_dist_bin + "nmi",
        )
        if ds_nasc.sizes:
            day_store.save_product(ds_nasc, day, "nasc")
            result["nasc_computed"] = True
    except Exception as e:
        logger.warning("NASC failed (may need GPS): %s", e)
        result["nasc_computed"] = False
    _release_memory()

    # --- Step 6: Echograms ---
    if config.plot_echogram:
        try:
            from exports.echograms import generate_echograms
            echogram_files = generate_echograms(
                ds_sv=ds_sv,
                ds_denoised=ds_denoised if config.denoise_enabled else None,
                ds_mvbs=ds_mvbs if result.get("mvbs_computed") else None,
                day=day,
                config=config,
            )
            result["echogram_files"] = echogram_files
        except Exception as e:
            logger.error("Echogram generation failed: %s", e, exc_info=True)
    _release_memory()

    # --- Step 7: Telemetry ---
    processing_time_ms = int((time.time() - start_time) * 1000)
    result["processing_time_ms"] = processing_time_ms

    if client:
        try:
            from exports.telemetry import send_processing_telemetry
            send_processing_telemetry(client, result, config)
        except Exception as e:
            logger.error("Telemetry send failed: %s", e)

    logger.info(
        "Pipeline complete for %s: %d pings in %dms",
        day, n_pings, processing_time_ms,
    )
    return result


# ═══════════════════════════════════════════════════════════════════════
# File trigger: raw file → full pipeline
# ═══════════════════════════════════════════════════════════════════════


async def process_raw_file_pipeline(
    file_path: str,
    config: "EdgeConfig",
    day_store: "DayStore",
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

    result = await process_echodata(echodata, config, day_store, client)
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


# ═══════════════════════════════════════════════════════════════════════
# On-demand: reprocess a day
# ═══════════════════════════════════════════════════════════════════════


async def process_day_pipeline(
    target_date: date,
    stages: List[str],
    config: "EdgeConfig",
    day_store: "DayStore",
    client: Optional["IoTHubModuleClient"] = None,
) -> Dict[str, Any]:
    """Reprocess a day's Sv data through selected stages.

    Parameters
    ----------
    target_date
        Day to reprocess.
    stages
        List of stages to run: ``"denoise"``, ``"mvbs"``, ``"nasc"``,
        ``"echograms"``.
    config
        Edge processing configuration.
    day_store
        Daily Zarr store manager.
    client
        IoT Hub module client.

    Returns
    -------
    dict
        Reprocessing result.
    """
    from process.denoise import denoise
    from process.mvbs import compute_mvbs
    from process.nasc import compute_nasc

    start = time.time()
    result: Dict[str, Any] = {"status": "ok", "day": target_date.isoformat(), "stages": stages}

    ds_sv = day_store.load_sv(target_date)
    if not ds_sv.sizes:
        return {"status": "error", "reason": f"no Sv data for {target_date}"}

    ds_denoised = ds_sv

    if "denoise" in stages and config.denoise_enabled:
        try:
            ds_denoised = denoise(ds_sv, use_gpu=config.use_gpu)
            day_store.save_product(ds_denoised, target_date, "sv_denoised")
            result["denoise"] = "ok"
        except Exception as e:
            logger.error("Day denoise failed: %s", e, exc_info=True)
            result["denoise"] = f"error: {e}"
        _release_memory()

    if "mvbs" in stages:
        try:
            ds_mvbs = compute_mvbs(
                ds_denoised,
                range_bin=config.mvbs_range_bin + "m",
                ping_time_bin=config.mvbs_ping_time_bin,
            )
            day_store.save_product(ds_mvbs, target_date, "mvbs")
            result["mvbs"] = "ok"
        except Exception as e:
            logger.error("Day MVBS failed: %s", e, exc_info=True)
            result["mvbs"] = f"error: {e}"
        _release_memory()

    if "nasc" in stages:
        try:
            ds_nasc = compute_nasc(
                ds_denoised,
                range_bin=config.nasc_range_bin + "m",
                dist_bin=config.nasc_dist_bin + "nmi",
            )
            day_store.save_product(ds_nasc, target_date, "nasc")
            result["nasc"] = "ok"
        except Exception as e:
            logger.warning("Day NASC failed: %s", e)
            result["nasc"] = f"error: {e}"
        _release_memory()

    if "echograms" in stages and config.plot_echogram:
        try:
            from exports.echograms import generate_echograms
            try:
                ds_mvbs = day_store.load_product(target_date, "mvbs")
                if not ds_mvbs.data_vars:
                    ds_mvbs = None
            except Exception:
                ds_mvbs = None
            echogram_files = generate_echograms(
                ds_sv=ds_sv,
                ds_denoised=ds_denoised if config.denoise_enabled else None,
                ds_mvbs=ds_mvbs,
                day=target_date,
                config=config,
            )
            result["echograms"] = echogram_files
        except Exception as e:
            logger.error("Day echograms failed: %s", e, exc_info=True)
        _release_memory()

    result["processing_time_ms"] = int((time.time() - start) * 1000)
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
