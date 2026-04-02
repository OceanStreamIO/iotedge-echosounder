"""Compute Nautical Area Scattering Coefficient (NASC) for edge processing."""

from __future__ import annotations

import logging

import numpy as np
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
        NASC dataset. A ``NASC_log`` variable is added (log10 of NASC).
    """
    from echopype.commongrid import compute_NASC
    from echopype.consolidate import add_depth

    # Ensure depth is present
    if "depth" not in ds_sv and "echo_range" in ds_sv:
        logger.info("Adding depth variable for NASC computation")
        try:
            ds_sv = add_depth(ds_sv, depth_offset=0.0)
        except Exception as e:
            logger.warning("Could not add depth for NASC: %s", e)

    # Ensure location variables exist
    if "latitude" not in ds_sv or "longitude" not in ds_sv:
        logger.warning("NASC requires latitude/longitude — skipping")
        return xr.Dataset()

    logger.info("Computing NASC (range_bin=%s, dist_bin=%s)", range_bin, dist_bin)

    ds_nasc = compute_NASC(
        ds_sv,
        range_bin=range_bin,
        dist_bin=dist_bin,
    )

    # Add log-transformed NASC for visualization
    if "NASC" in ds_nasc:
        ds_nasc["NASC_log"] = np.log10(ds_nasc["NASC"].where(ds_nasc["NASC"] > 0))
        ds_nasc["NASC_log"].attrs = {
            "long_name": "log10(NASC)",
            "units": "log10(m2 nmi-2)",
        }

    logger.info("NASC complete: %s", dict(ds_nasc.sizes))
    return ds_nasc
