"""Seabed detection and masking for edge Sv datasets.

Uses ``oceanstream.echodata.seabed`` to detect the seabed line and mask
samples below it.  Disabled by default (``seabed_enabled=False`` in config).
"""

from __future__ import annotations

import logging
from typing import Literal

import xarray as xr

logger = logging.getLogger("oceanstream")


def apply_seabed(
    ds_sv: xr.Dataset,
    *,
    method: Literal["maxSv", "deltaSv", "ariza", "composite", "blackwell"] = "ariza",
    max_range: float = 1000.0,
) -> xr.Dataset:
    """Detect the seabed and mask samples below it.

    Parameters
    ----------
    ds_sv
        Sv (or denoised Sv) dataset.
    method
        Detection algorithm passed to ``detect_seabed()``.
    max_range
        Maximum range (metres) to search for the seabed.

    Returns
    -------
    xr.Dataset
        Sv dataset with below-seabed samples set to NaN.
    """
    from oceanstream.echodata.seabed import detect_seabed, mask_seabed

    logger.info("Detecting seabed (method=%s, max_range=%.0fm)", method, max_range)

    seabed_result = detect_seabed(ds_sv, method=method, r1=max_range)
    ds_masked = mask_seabed(ds_sv, seabed_result)

    logger.info("Seabed masking complete")
    return ds_masked
