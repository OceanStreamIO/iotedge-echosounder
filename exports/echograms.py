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
    output_subdir: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Generate echogram PNGs for all available products.

    Renders to a temporary directory and returns a list of dicts with
    ``filename`` and ``data`` (PNG bytes) so the caller can persist them
    through the appropriate storage backend (local or blob).

    Returns
    -------
    list of dict
        Each dict has ``{"filename": str, "data": bytes}``.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        files: List[Dict[str, Any]] = []

        # Source Sv echograms (per channel)
        try:
            sv_files = _plot_sv_echograms(ds_sv, tmp_path, prefix="sv")
            for fp in sv_files:
                files.append({"filename": Path(fp).name, "data": Path(fp).read_bytes()})
        except Exception as e:
            logger.error("Source Sv echogram failed: %s", e)

        # Denoised echograms
        if ds_denoised is not None:
            try:
                dn_files = _plot_sv_echograms(ds_denoised, tmp_path, prefix="denoised")
                for fp in dn_files:
                    files.append({"filename": Path(fp).name, "data": Path(fp).read_bytes()})
            except Exception as e:
                logger.error("Denoised echogram failed: %s", e)

        # MVBS echogram
        if ds_mvbs is not None and "Sv" in ds_mvbs:
            try:
                mvbs_file = _plot_gridded_echogram(ds_mvbs, tmp_path, "mvbs")
                if mvbs_file:
                    files.append({"filename": Path(mvbs_file).name, "data": Path(mvbs_file).read_bytes()})
            except Exception as e:
                logger.error("MVBS echogram failed: %s", e)

        logger.info("Generated %d echogram images", len(files))
        return files


import re


def _extract_depth(ds: xr.Dataset, channel, *, n_range: int) -> Optional[np.ndarray]:
    """Extract a 1D depth/range array from a dataset.

    Checks ``depth`` and ``echo_range`` as data variables or coordinates,
    reduces from 3D ``(channel, ping_time, range_sample)`` to 1D,
    trims to *n_range* samples, and strips trailing NaN values so the
    result is safe for ``pcolormesh``.
    """
    for var_name in ("depth", "echo_range"):
        if var_name not in ds:
            continue
        try:
            arr = ds[var_name]
            if channel is not None and "channel" in arr.dims:
                arr = arr.sel(channel=channel)
            if "ping_time" in arr.dims:
                arr = arr.isel(ping_time=0)
            vals = arr.values.flatten()
            # Trim to data length
            vals = vals[:n_range]
            # Drop trailing NaNs (different transducers have different max range)
            valid_mask = ~np.isnan(vals)
            if not valid_mask.any():
                continue
            last_valid = np.where(valid_mask)[0][-1]
            vals = vals[: last_valid + 1]
            if len(vals) > 1:
                return vals
        except Exception:
            continue
    return None


def _freq_label(ds: xr.Dataset, channel) -> str:
    """Get a human-readable frequency label like '38kHz' for a channel."""
    try:
        if "frequency_nominal" in ds.coords:
            freq = float(ds["frequency_nominal"].sel(channel=channel).values)
            if freq >= 1000:
                return f"{int(freq / 1000)}kHz"
            return f"{int(freq)}Hz"
    except Exception:
        pass
    # Parse frequency from channel string like "WBT 400528-15 ES38-7_ES" → 38kHz
    ch_str = str(channel)
    m = re.search(r"ES(\d+)", ch_str)
    if m:
        return f"{m.group(1)}kHz"
    # Fallback: sanitize the channel string
    return ch_str.replace(" ", "_").replace("/", "_")[:30]


def _plot_sv_echograms(
    ds: xr.Dataset,
    output_dir: Path,
    prefix: str = "sv",
) -> List[str]:
    """Plot Sv echogram per channel with real time/depth axes."""
    files = []

    if "Sv" not in ds:
        return files

    channels = ds["channel"].values if "channel" in ds.dims else [None]

    for ch in channels:
        if ch is not None:
            sv_data = ds["Sv"].sel(channel=ch)
            freq_lbl = _freq_label(ds, ch)
        else:
            sv_data = ds["Sv"]
            freq_lbl = "all"

        # Get 2D data (ping_time × range/depth)
        data = sv_data.values
        if data.ndim != 2:
            continue

        # Build real axes where possible
        ping_times = None
        if "ping_time" in sv_data.dims:
            try:
                ping_times = sv_data["ping_time"].values
            except Exception:
                pass

        depth_vals = _extract_depth(ds, ch, n_range=data.shape[1])

        fig, ax = plt.subplots(figsize=(14, 6))

        if ping_times is not None and depth_vals is not None:
            import matplotlib.dates as mdates
            # Trim data to match depth array (trailing NaNs were stripped)
            plot_data = data[:, : len(depth_vals)]
            time_num = mdates.date2num(ping_times.astype("datetime64[ms]").astype("O"))
            im = ax.pcolormesh(
                time_num, depth_vals, plot_data.T,
                vmin=-80, vmax=-30, cmap="ocean_r", shading="auto",
            )
            ax.xaxis_date()
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            fig.autofmt_xdate()
            ax.set_xlabel("Time (UTC)")
            ax.set_ylabel("Depth (m)")
        elif depth_vals is not None:
            plot_data = data[:, : len(depth_vals)]
            im = ax.pcolormesh(
                np.arange(plot_data.shape[0]), depth_vals, plot_data.T,
                vmin=-80, vmax=-30, cmap="ocean_r", shading="auto",
            )
            ax.set_xlabel("Ping")
            ax.set_ylabel("Depth (m)")
        else:
            im = ax.pcolormesh(
                np.arange(data.shape[0]), np.arange(data.shape[1]), data.T,
                vmin=-80, vmax=-30, cmap="ocean_r", shading="auto",
            )
            ax.set_xlabel("Ping")
            ax.set_ylabel("Range sample")

        ax.set_title(f"{prefix.replace('_', ' ').title()} — {freq_lbl}")
        ax.invert_yaxis()
        plt.colorbar(im, ax=ax, label="Sv (dB re 1 m⁻¹)")

        filename = f"{prefix}_{freq_lbl}.png"
        filepath = output_dir / filename
        fig.savefig(filepath, dpi=120, bbox_inches="tight")
        plt.close(fig)
        files.append(str(filepath))

    return files


def _plot_gridded_echogram(
    ds: xr.Dataset,
    output_dir: Path,
    product: str,
) -> Optional[str]:
    """Plot a gridded (MVBS/NASC) echogram with real axes where available."""
    var_name = "Sv" if "Sv" in ds else "NASC" if "NASC" in ds else None
    if var_name is None:
        return None

    channels = ds["channel"].values if "channel" in ds.dims else [None]
    ch = channels[0]
    freq_lbl = _freq_label(ds, ch) if ch is not None else ""

    if ch is not None:
        data = ds[var_name].sel(channel=ch).values
    else:
        data = ds[var_name].values

    if data.ndim != 2:
        return None

    fig, ax = plt.subplots(figsize=(14, 6))
    vmin, vmax = (-80, -30) if var_name == "Sv" else (0, np.nanpercentile(data, 95))

    # Try real time axis
    ping_times = None
    if "ping_time" in ds.coords:
        try:
            import matplotlib.dates as mdates
            ping_times = ds["ping_time"].values
            time_num = mdates.date2num(ping_times.astype("datetime64[ms]").astype("O"))
        except Exception:
            ping_times = None

    depth_vals = _extract_depth(ds, ch, n_range=data.shape[1])

    if ping_times is not None and depth_vals is not None:
        plot_data = data[:, : len(depth_vals)]
        im = ax.pcolormesh(
            time_num, depth_vals, plot_data.T,
            vmin=vmin, vmax=vmax, cmap="ocean_r", shading="auto",
        )
        ax.xaxis_date()
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        fig.autofmt_xdate()
        ax.set_xlabel("Time (UTC)")
        ax.set_ylabel("Depth (m)")
    elif ping_times is not None:
        im = ax.pcolormesh(
            time_num, np.arange(data.shape[1]), data.T,
            vmin=vmin, vmax=vmax, cmap="ocean_r", shading="auto",
        )
        ax.xaxis_date()
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        fig.autofmt_xdate()
        ax.set_xlabel("Time (UTC)")
        ax.set_ylabel("Range bin")
    else:
        im = ax.pcolormesh(
            np.arange(data.shape[0]), np.arange(data.shape[1]), data.T,
            vmin=vmin, vmax=vmax, cmap="ocean_r", shading="auto",
        )
        ax.set_xlabel("Time bin")
        ax.set_ylabel("Range bin")

    title = product.upper()
    if freq_lbl:
        title += f" — {freq_lbl}"
    ax.set_title(title)
    ax.invert_yaxis()
    plt.colorbar(im, ax=ax, label=f"{var_name} (dB)" if var_name == "Sv" else var_name)

    filename = f"{product}.png"
    filepath = output_dir / filename
    fig.savefig(filepath, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return str(filepath)
