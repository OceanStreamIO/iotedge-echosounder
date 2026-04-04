"""Compute Mean Volume Backscattering Strength (MVBS) for edge processing.

Uses oceanstream's xarray-2026-safe ``compute_mvbs`` wrapper.
"""

from __future__ import annotations

import logging

import xarray as xr

logger = logging.getLogger("oceanstream")


def compute_mvbs(
    ds_sv: xr.Dataset,
    *,
    range_bin: str = "0.5m",
    ping_time_bin: str = "10s",
) -> xr.Dataset:
    """Compute MVBS from an Sv dataset.

    Parameters
    ----------
    ds_sv
        Sv or denoised Sv dataset.
    range_bin
        Range bin size (e.g. ``"0.5m"``, ``"1m"``).
    ping_time_bin
        Time bin size (e.g. ``"10s"``, ``"20s"``).

    Returns
    -------
    xr.Dataset
        MVBS dataset.
    """
    from oceanstream.echodata.compute import compute_mvbs as os_compute_mvbs

    logger.info("Computing MVBS (range_bin=%s, ping_time_bin=%s)", range_bin, ping_time_bin)

    ds_mvbs = os_compute_mvbs(
        ds_sv,
        range_bin=range_bin,
        ping_time_bin=ping_time_bin,
    )

    logger.info("MVBS complete: %s", dict(ds_mvbs.sizes))
    return ds_mvbs
