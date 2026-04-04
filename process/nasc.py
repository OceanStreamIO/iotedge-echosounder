"""Compute Nautical Area Scattering Coefficient (NASC) for edge processing.

Uses oceanstream's ``compute_nasc`` which adds ``NASC_log`` automatically.
"""

from __future__ import annotations

import logging

import xarray as xr

logger = logging.getLogger("oceanstream")


def compute_nasc(
    ds_sv: xr.Dataset,
    *,
    range_bin: str = "10m",
    dist_bin: str = "0.5nmi",
) -> xr.Dataset:
    """Compute NASC from an Sv dataset.

    Parameters
    ----------
    ds_sv
        Sv or denoised Sv dataset.  Must have ``latitude`` and ``longitude``
        variables (from ``add_location`` or GPS interpolation).
    range_bin
        Range bin size (e.g. ``"10m"``).
    dist_bin
        Distance bin size in nautical miles (e.g. ``"0.5nmi"``).

    Returns
    -------
    xr.Dataset
        NASC dataset with ``NASC_log`` variable.
    """
    from oceanstream.echodata.compute import compute_nasc as os_compute_nasc

    # Ensure location variables exist
    if "latitude" not in ds_sv or "longitude" not in ds_sv:
        logger.warning("NASC requires latitude/longitude — skipping")
        return xr.Dataset()

    logger.info("Computing NASC (range_bin=%s, dist_bin=%s)", range_bin, dist_bin)

    ds_nasc = os_compute_nasc(
        ds_sv,
        range_bin=range_bin,
        dist_bin=dist_bin,
    )

    logger.info("NASC complete: %s", dict(ds_nasc.sizes))
    return ds_nasc
