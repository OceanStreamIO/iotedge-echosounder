"""Generate a structured Markdown processing report.

Designed for RAG retrieval and LLM consumption:
- Structured headings for easy chunking
- Machine-readable metadata in bullet points
- Inline echogram image references for vision model analysis
- Per-file detail sections for granular retrieval
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    from config import EdgeConfig

logger = logging.getLogger("oceanstream")


def _fmt_freq(hz: float) -> str:
    """Format a frequency in Hz to a human-readable string."""
    if hz >= 1000:
        return f"{int(hz / 1000)} kHz"
    return f"{int(hz)} Hz"


def _storage_path(path: str) -> str:
    """Normalise a storage-relative path for use in the report.

    Pipeline result paths are already storage-relative (e.g.
    ``echograms/2023-06-26/segments/…/sv_38kHz.png``).  For paths that
    are still absolute filesystem paths (legacy), try to strip a common
    base prefix.
    """
    # Already relative — use as-is
    if not Path(path).is_absolute():
        return path
    # Fallback: return the path after the first known container segment
    for part in ("echograms/", "echodata/", "processed/", "reports/"):
        idx = path.find(part)
        if idx >= 0:
            return path[idx:]
    return path


def generate_md_report(
    results: List[Dict[str, Any]],
    config: "EdgeConfig",
    total_time_s: float,
    input_dir: str = "",
) -> str:
    """Generate a Markdown report from pipeline results.

    Returns the report as a UTF-8 string so the caller can persist it
    through the storage backend (local filesystem or blob storage).

    Parameters
    ----------
    results
        List of per-file result dicts from the pipeline.
    config
        The EdgeConfig used for this run.
    total_time_s
        Total wall-clock time for the run.
    input_dir
        Source directory of raw files.
    """
    lines: list[str] = []

    def w(line: str = ""):
        lines.append(line)

    # ── Header ──
    survey_label = config.survey_id or "Standalone Run"
    w(f"# Processing Report: {survey_label}")
    w()
    w(f"> Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')} "
      f"by iotedge-echosounder processing pipeline")
    w()

    # ── Summary ──
    ok_results = [r for r in results if r.get("status") == "ok"]
    failed_results = [r for r in results if r.get("status") == "error"]
    skipped_results = [r for r in results if r.get("status") == "skipped"]
    total_pings = sum(r.get("n_pings", 0) for r in ok_results)

    # Collect all unique frequencies across files
    all_freqs: set[float] = set()
    for r in ok_results:
        for f in r.get("frequencies_hz", []):
            all_freqs.add(f)

    w("## Summary")
    w()
    w(f"- **Survey ID**: {config.survey_id}")
    if config.survey_name:
        w(f"- **Survey name**: {config.survey_name}")
    w(f"- **Input directory**: `{input_dir}`")
    w(f"- **Files processed**: {len(ok_results)} ok, "
      f"{len(failed_results)} failed, {len(skipped_results)} skipped "
      f"(of {len(results)} total)")
    w(f"- **Total pings**: {total_pings:,}")
    w(f"- **Sonar model**: {config.sonar_model}")
    if all_freqs:
        freq_str = ", ".join(_fmt_freq(f) for f in sorted(all_freqs))
        w(f"- **Frequencies**: {freq_str}")
    w(f"- **Total processing time**: {total_time_s:.1f}s")
    w()

    # ── Pipeline Configuration ──
    w("## Pipeline Configuration")
    w()
    w(f"- **Waveform mode**: {config.waveform_mode}")
    w(f"- **Encode mode**: {config.encode_mode}")
    w(f"- **Depth offset**: {config.depth_offset} m")
    w(f"- **GPU acceleration**: {'enabled' if config.use_gpu else 'disabled'}")
    w(f"- **Denoise**: {'enabled' if config.denoise_enabled else 'disabled'}"
      + (f" ({config.denoise_methods})" if config.denoise_enabled else ""))
    w(f"- **MVBS**: {'enabled' if config.mvbs_enabled else 'disabled'}"
      + (f" (range_bin={config.mvbs_range_bin}m, "
         f"ping_time_bin={config.mvbs_ping_time_bin})" if config.mvbs_enabled else ""))
    w(f"- **NASC**: {'enabled' if config.nasc_enabled else 'disabled'}"
      + (f" (range_bin={config.nasc_range_bin}m, "
         f"dist_bin={config.nasc_dist_bin}nmi)" if config.nasc_enabled else ""))
    w(f"- **Seabed detection**: {'enabled' if config.seabed_enabled else 'disabled'}"
      + (f" (method={config.seabed_method}, "
         f"max_range={config.seabed_max_range}m)" if config.seabed_enabled else ""))
    w(f"- **Echograms**: {'enabled' if config.plot_echogram else 'disabled'}")
    w()

    # ── Per-file Sections ──
    w("## Files Processed")
    w()

    for r in results:
        fname = r.get("source_file", "unknown")
        status = r.get("status", "unknown")
        w(f"### {fname}")
        w()

        if status == "error":
            w(f"- **Status**: ❌ FAILED")
            w(f"- **Error**: {r.get('error', 'unknown')}")
            w(f"- **Processing time**: {r.get('total_time_ms', 0)} ms")
            w()
            continue

        if status == "skipped":
            w(f"- **Status**: ⏭ Skipped — {r.get('reason', '')}")
            w()
            continue

        # Successful processing
        w(f"- **Status**: ✅ OK")
        w(f"- **Day**: {r.get('day', '')}")
        w(f"- **Segment**: {r.get('segment', '')}")
        w(f"- **Pings**: {r.get('n_pings', 0):,}")
        w(f"- **Channels**: {r.get('n_channels', '')}")

        if r.get("frequencies_hz"):
            freq_str = ", ".join(_fmt_freq(f) for f in r["frequencies_hz"])
            w(f"- **Frequencies**: {freq_str}")

        if r.get("lat_range"):
            w(f"- **Latitude**: [{r['lat_range'][0]:.4f}, {r['lat_range'][1]:.4f}]")
        if r.get("lon_range"):
            w(f"- **Longitude**: [{r['lon_range'][0]:.4f}, {r['lon_range'][1]:.4f}]")

        w(f"- **Processing time**: {r.get('total_time_ms', r.get('processing_time_ms', 0))} ms")

        # Products
        products = r.get("products", [])
        if products:
            w(f"- **Products**: {', '.join(products)}")

        # Echodata path (storage-relative)
        if r.get("echodata_path"):
            w(f"- **Echodata**: `{_storage_path(r['echodata_path'])}`")

        # Sv path (storage-relative)
        if r.get("sv_path"):
            w(f"- **Sv data**: `{_storage_path(r['sv_path'])}`")

        w()

        # Echogram images
        echogram_files = r.get("echogram_files", [])
        if echogram_files:
            w("#### Echograms")
            w()
            for ef in echogram_files:
                sp = _storage_path(ef)
                # Derive a descriptive alt text from the filename
                stem = Path(ef).stem
                alt = stem.replace("_", " ").title()
                w(f"![{alt}]({sp})")
                w()
            w()

    # ── Data Quality Notes ──
    if failed_results or skipped_results:
        w("## Data Quality Notes")
        w()
        for r in failed_results:
            w(f"- **{r.get('source_file', '?')}**: Failed — {r.get('error', 'unknown error')}")
        for r in skipped_results:
            w(f"- **{r.get('source_file', '?')}**: Skipped — {r.get('reason', '')}")
        # Check for non-fatal warnings in ok results
        for r in ok_results:
            warnings = []
            if r.get("denoise") and r["denoise"] != "ok" and "error" in str(r.get("denoise", "")):
                warnings.append(f"denoise: {r['denoise']}")
            if r.get("nasc") and "skipped" in str(r.get("nasc", "")):
                warnings.append(f"NASC: {r['nasc']}")
            if r.get("seabed") and r["seabed"] != "ok" and "error" in str(r.get("seabed", "")):
                warnings.append(f"seabed: {r['seabed']}")
            if warnings:
                w(f"- **{r.get('source_file', '?')}**: {'; '.join(warnings)}")
        w()

    # ── Artifacts ──
    w("## Output Artifacts")
    w()
    w(f"- **Echodata (converted)**: `{config.converted_container}/`")
    w(f"- **Processed products**: `{config.processed_container}/`")
    w(f"- **Echograms**: `{config.echogram_container}/`")
    w(f"- **This report**: `reports/`")
    w()

    content = "\n".join(lines)
    logger.info("Markdown report generated (%d lines)", len(lines))
    return content
