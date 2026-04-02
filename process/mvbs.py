"""Compute Mean Volume Backscattering Strength (MVBS) for edge processing."""

from __future__ import annotations

import logging

import xarray as xr

logger = logging.getLogger("oceanstream")


def compute_mvbs(
    ds_sv: xr.Dataset,
    *,
    range_var: str = "echo_range",
    range_bin: str = "0.5m",
    ping_time_bin: str = "10s",
) -> xr.Dataset:
    """Compute MVBS from an Sv dataset.

    Parameters
    ----------
    ds_sv
        Sv or denoised Sv dataset.
    range_var
        Range variable to bin over (``"echo_range"`` or ``"depth"``).
    range_bin
        Range bin size (e.g. ``"0.5m"``, ``"20m"``).
    ping_time_bin
        Time bin size (e.g. ``"10s"``, ``"20s"``).

    Returns
    -------
    xr.Dataset
        MVBS dataset.
    """
    from echopype.commongrid import compute_MVBS

    logger.info("Computing MVBS (range_bin=%s, ping_time_bin=%s)", range_bin, ping_time_bin)

    ds_mvbs = compute_MVBS(
        ds_sv,
        range_var=range_var,
        range_bin=range_bin,
        ping_time_bin=ping_time_bin,
    )

    logger.info("MVBS complete: %s", dict(ds_mvbs.sizes))
    return ds_mvbs
