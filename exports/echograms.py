"""Generate echogram PNG images for edge visualization.

Produces echograms for Sv, denoised Sv, MVBS, and NASC products.
Uses matplotlib with Agg backend (no display needed on edge device).
"""

from __future__ import annotations

import logging
import tempfile
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

if TYPE_CHECKING:
    from config import EdgeConfig

logger = logging.getLogger("oceanstream")


def generate_echograms(
    ds_sv: xr.Dataset,
    *,
    ds_denoised: Optional[xr.Dataset] = None,
    ds_mvbs: Optional[xr.Dataset] = None,
    day: date,
    config: "EdgeConfig",
) -> List[str]:
    """Generate echogram PNGs for all available products.

    Returns a list of saved file paths.
    """
    output_dir = Path(config.output_base_path) / day.isoformat() / "echograms"
    output_dir.mkdir(parents=True, exist_ok=True)

    files: List[str] = []

    # Source Sv echograms (per channel)
    try:
        sv_files = _plot_sv_echograms(ds_sv, output_dir, prefix="sv")
        files.extend(sv_files)
    except Exception as e:
        logger.error("Source Sv echogram failed: %s", e)

    # Denoised echograms
    if ds_denoised is not None:
        try:
            dn_files = _plot_sv_echograms(ds_denoised, output_dir, prefix="denoised")
            files.extend(dn_files)
        except Exception as e:
            logger.error("Denoised echogram failed: %s", e)

    # MVBS echogram
    if ds_mvbs is not None and "Sv" in ds_mvbs:
        try:
            mvbs_file = _plot_gridded_echogram(ds_mvbs, output_dir, "mvbs")
            if mvbs_file:
                files.append(mvbs_file)
        except Exception as e:
            logger.error("MVBS echogram failed: %s", e)

    logger.info("Generated %d echogram files in %s", len(files), output_dir)
    return files


def _plot_sv_echograms(
    ds: xr.Dataset,
    output_dir: Path,
    prefix: str = "sv",
) -> List[str]:
    """Plot Sv echogram per channel."""
    files = []

    if "Sv" not in ds:
        return files

    channels = ds["channel"].values if "channel" in ds.dims else [None]

    for ch in channels:
        if ch is not None:
            sv_data = ds["Sv"].sel(channel=ch)
            ch_label = str(ch).replace(" ", "_").replace("/", "_")
        else:
            sv_data = ds["Sv"]
            ch_label = "all"

        # Get 2D data (ping_time × range_sample or depth)
        data = sv_data.values
        if data.ndim != 2:
            continue

        fig, ax = plt.subplots(figsize=(14, 6))
        im = ax.pcolormesh(
            np.arange(data.shape[0]),
            np.arange(data.shape[1]),
            data.T,
            vmin=-80,
            vmax=-30,
            cmap="viridis",
            shading="auto",
        )
        ax.set_xlabel("Ping")
        ax.set_ylabel("Range sample")
        ax.set_title(f"{prefix} — {ch_label}")
        ax.invert_yaxis()
        plt.colorbar(im, ax=ax, label="Sv (dB)")

        filename = f"{prefix}_{ch_label}.png"
        filepath = output_dir / filename
        fig.savefig(filepath, dpi=100, bbox_inches="tight")
        plt.close(fig)
        files.append(str(filepath))

    return files


def _plot_gridded_echogram(
    ds: xr.Dataset,
    output_dir: Path,
    product: str,
) -> Optional[str]:
    """Plot a gridded (MVBS/NASC) echogram."""
    var_name = "Sv" if "Sv" in ds else "NASC" if "NASC" in ds else None
    if var_name is None:
        return None

    channels = ds["channel"].values if "channel" in ds.dims else [None]
    ch = channels[0]

    if ch is not None:
        data = ds[var_name].sel(channel=ch).values
    else:
        data = ds[var_name].values

    if data.ndim != 2:
        return None

    fig, ax = plt.subplots(figsize=(14, 6))
    vmin, vmax = (-80, -30) if var_name == "Sv" else (0, np.nanpercentile(data, 95))

    im = ax.pcolormesh(
        np.arange(data.shape[0]),
        np.arange(data.shape[1]),
        data.T,
        vmin=vmin,
        vmax=vmax,
        cmap="viridis",
        shading="auto",
    )
    ax.set_xlabel("Time bin")
    ax.set_ylabel("Range bin")
    ax.set_title(f"{product.upper()}")
    ax.invert_yaxis()
    plt.colorbar(im, ax=ax, label=f"{var_name}")

    filename = f"{product}.png"
    filepath = output_dir / filename
    fig.savefig(filepath, dpi=100, bbox_inches="tight")
    plt.close(fig)
    return str(filepath)
