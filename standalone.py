#!/usr/bin/env python3
"""Standalone entry point for the echosounder processing pipeline.

Runs the full processing pipeline locally without Azure IoT Edge runtime.
Processes raw echosounder files from a directory and produces all outputs
inside a single **campaign container** (``output/{campaign_container}/``):

::

    output/{campaign_container}/
        echodata/{stem}.zarr
        processed/{stem}/sv.zarr
        processed/{stem}/sv_denoised.zarr
        processed/{stem}/mvbs.zarr
        processed/{stem}/metadata.json
        echograms/{stem}/sv_38kHz.png
        reports/{survey_id}_report.md

Usage::

    python standalone.py \\
        --input-dir ../sd-data-ingest/raw_data/HB2302 \\
        --output-dir ./output \\
        --sonar-model EK80 \\
        --survey-id HB2302
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
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
for _noisy in ("distributed", "dask", "fsspec", "zarr", "urllib3"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process raw echosounder files locally (standalone mode).",
    )
    parser.add_argument(
        "--input-dir", required=True, type=Path,
        help="Directory containing .raw echosounder files.",
    )
    parser.add_argument(
        "--output-dir", default=Path("./output"), type=Path,
        help="Base output directory (default: ./output).",
    )
    parser.add_argument(
        "--sonar-model", default="EK80",
        help="Echosounder model: EK80, EK60, AZFP (default: EK80).",
    )
    parser.add_argument(
        "--survey-id", default="",
        help="Survey/campaign identifier for organizing output.",
    )
    parser.add_argument(
        "--survey-name", default="",
        help="Human-readable survey name.",
    )
    parser.add_argument(
        "--waveform-mode", default="CW",
        help="Waveform mode: CW or BB (default: CW).",
    )
    parser.add_argument(
        "--encode-mode", default="power",
        help="Encode mode: power or complex (default: power).",
    )
    parser.add_argument(
        "--depth-offset", default=0.0, type=float,
        help="Transducer depth offset in metres (default: 0.0).",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=False,
        help="Enable GPU acceleration (requires CUDA).",
    )
    parser.add_argument(
        "--no-denoise", action="store_true", default=False,
        help="Disable denoising.",
    )
    parser.add_argument(
        "--no-mvbs", action="store_true", default=False,
        help="Disable MVBS computation.",
    )
    parser.add_argument(
        "--enable-nasc", action="store_true", default=False,
        help="Enable NASC computation (requires GPS data).",
    )
    parser.add_argument(
        "--enable-seabed", action="store_true", default=False,
        help="Enable seabed detection and masking.",
    )
    parser.add_argument(
        "--no-echograms", action="store_true", default=False,
        help="Disable echogram generation.",
    )
    parser.add_argument(
        "--max-files", default=None, type=int,
        help="Process at most N files (useful for testing).",
    )
    return parser.parse_args()


async def run_pipeline(args: argparse.Namespace) -> None:
    from azure_handler.storage import LocalStorage
    from config import EdgeConfig
    from exports.report_md import generate_md_report
    from process.pipeline import process_raw_file_pipeline
    from process.segment_store import SegmentStore

    # --- Build config ---
    config = EdgeConfig.from_standalone(
        sonar_model=args.sonar_model,
        waveform_mode=args.waveform_mode,
        encode_mode=args.encode_mode,
        depth_offset=args.depth_offset,
        survey_id=args.survey_id,
        survey_name=args.survey_name or args.survey_id,
        use_gpu=args.use_gpu,
        output_base_path=str(args.output_dir),
        denoise_enabled=not args.no_denoise,
        mvbs_enabled=not args.no_mvbs,
        nasc_enabled=args.enable_nasc,
        seabed_enabled=args.enable_seabed,
        plot_echogram=not args.no_echograms,
    )
    logging.getLogger("oceanstream").setLevel(
        getattr(logging, config.log_level.upper(), logging.INFO)
    )

    logger.info(
        "Standalone mode: sonar=%s, survey=%s, gpu=%s",
        config.sonar_model, config.survey_id, config.use_gpu,
    )

    # --- Storage ---
    # Pipeline paths are "{campaign_container}/processed/{stem}/..." etc.
    # With LocalStorage, base_path is the root output dir and the campaign
    # container becomes a subdirectory, yielding:
    #   output/{campaign_container}/processed/{stem}/sv.zarr
    storage = LocalStorage(base_path=str(args.output_dir))
    segment_store = SegmentStore(
        storage,
        container=config.campaign_container,
        processed_subfolder=config.processed_container,
    )

    # --- Discover raw files ---
    input_dir = args.input_dir.resolve()
    raw_files = sorted(input_dir.glob("*.raw"))
    if not raw_files:
        logger.error("No .raw files found in %s", input_dir)
        sys.exit(1)

    if args.max_files:
        raw_files = raw_files[: args.max_files]

    logger.info("Found %d raw files in %s", len(raw_files), input_dir)

    # --- Process each file ---
    all_results = []
    total_start = time.time()

    for i, raw_file in enumerate(raw_files, 1):
        logger.info("━━━ File %d/%d: %s ━━━", i, len(raw_files), raw_file.name)
        file_start = time.time()

        try:
            # Full pipeline: convert → save echodata → Sv → denoise → MVBS → echograms
            # All persistence goes through the storage backend.
            result = await process_raw_file_pipeline(
                str(raw_file), config, segment_store, client=None,
            )

            # Extract additional metadata from echodata for the report
            # (re-open the converted file briefly)
            _enrich_result_metadata(result, raw_file, config)

            all_results.append(result)
            logger.info(
                "✓ %s — %d pings, %dms",
                raw_file.name, result.get("n_pings", 0), result.get("total_time_ms", 0),
            )

        except Exception as e:
            logger.error("✗ %s — FAILED: %s", raw_file.name, e, exc_info=True)
            all_results.append({
                "source_file": raw_file.name,
                "status": "error",
                "error": str(e),
                "total_time_ms": int((time.time() - file_start) * 1000),
            })

    total_time = time.time() - total_start

    # --- Generate and persist report ---
    report_filename = f"{config.survey_id or 'standalone'}_report.md"
    report_storage_path = f"{config.campaign_container}/reports/{report_filename}"
    report_content = generate_md_report(
        results=all_results,
        config=config,
        total_time_s=total_time,
        input_dir=str(input_dir),
    )
    storage.save_file(report_content.encode("utf-8"), report_storage_path)
    logger.info("Report written to %s", report_storage_path)

    # --- Summary ---
    ok = sum(1 for r in all_results if r.get("status") == "ok")
    failed = sum(1 for r in all_results if r.get("status") == "error")
    skipped = sum(1 for r in all_results if r.get("status") == "skipped")
    logger.info(
        "━━━ Done: %d ok, %d failed, %d skipped in %.1fs ━━━",
        ok, failed, skipped, total_time,
    )


def _enrich_result_metadata(result: dict, raw_file: Path, config) -> None:
    """Add channel/frequency/location metadata to a pipeline result.

    Re-opens the converted EchoData (stored through the storage backend)
    to read instrument metadata that isn't carried through to Sv.
    """
    try:
        from echopype.convert.api import open_raw
        ed = open_raw(str(raw_file), sonar_model=config.sonar_model)

        try:
            result["channels"] = [
                str(ch)
                for ch in ed["Sonar/Beam_group1"]["beam_type"].coords["channel"].values
            ]
        except Exception:
            result.setdefault("channels", [])

        try:
            freqs = ed["Sonar/Beam_group1"]["frequency_nominal"].values
            result["frequencies_hz"] = [float(f) for f in freqs]
        except Exception:
            result.setdefault("frequencies_hz", [])

        try:
            plat = ed["Platform"]
            lats = plat["latitude"].values
            lons = plat["longitude"].values
            import numpy as np
            valid_lats = lats[~(lats == 0)]
            valid_lons = lons[~(lons == 0)]
            if len(valid_lats) > 0:
                result["lat_range"] = [float(valid_lats.min()), float(valid_lats.max())]
                result["lon_range"] = [float(valid_lons.min()), float(valid_lons.max())]
        except Exception:
            pass
    except Exception:
        pass


def main():
    args = parse_args()
    asyncio.run(run_pipeline(args))


if __name__ == "__main__":
    main()
