"""Compute volume backscattering strength (Sv) with GPU acceleration.

Wraps ``echopype.calibrate.compute_Sv`` and enrichment (add depth,
location, split-beam angles) into a single function suitable for edge
processing.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import xarray as xr

if TYPE_CHECKING:
    from echopype.echodata.echodata import EchoData

logger = logging.getLogger("oceanstream")


def compute_sv(
    echodata: "EchoData",
    *,
    waveform_mode: str = "CW",
    encode_mode: str = "power",
    use_gpu: bool = True,
    depth_offset: float = 0.0,
) -> xr.Dataset:
    """Compute Sv from EchoData with optional GPU acceleration.

    Parameters
    ----------
    echodata
        Converted EchoData object.
    waveform_mode
        ``"CW"`` or ``"BB"`` (broadband).
    encode_mode
        ``"power"`` or ``"complex"``.
    use_gpu
        Whether to use CUDA GPU acceleration (``True``, ``False``, or
        ``"auto"``).
    depth_offset
        Transducer depth offset in metres.

    Returns
    -------
    xr.Dataset
        Sv dataset with depth, location, and split-beam angle variables
        added where available.
    """
    from echopype.calibrate import compute_Sv
    from echopype.consolidate import add_depth

    # Compute Sv
    compute_kwargs = {}
    if echodata.sonar_model in ("EK80", "ES80", "EA640"):
        compute_kwargs["waveform_mode"] = waveform_mode
        compute_kwargs["encode_mode"] = encode_mode

    gpu_flag = "auto" if use_gpu else False
    logger.info(
        "Computing Sv (model=%s, waveform=%s, encode=%s, gpu=%s)",
        echodata.sonar_model, waveform_mode, encode_mode, gpu_flag,
    )
    ds_sv = compute_Sv(echodata, use_gpu=gpu_flag, **compute_kwargs)

    # Add depth
    logger.info("Adding depth (offset=%.1fm)", depth_offset)
    try:
        ds_sv = add_depth(ds_sv, depth_offset=depth_offset)
    except Exception as e:
        logger.warning("Failed to add depth: %s", e)

    # Add location from EchoData Platform group
    try:
        from echopype.consolidate import add_location
        ds_sv = add_location(ds_sv, echodata)
    except Exception as e:
        logger.warning("Failed to add location: %s", e)

    # Add split-beam angles if available
    try:
        from echopype.consolidate import add_splitbeam_angle
        ds_sv = add_splitbeam_angle(ds_sv, echodata)
    except Exception as e:
        logger.debug("Split-beam angle not available: %s", e)

    # Drop all-NaN pings
    ds_sv = ds_sv.dropna(dim="ping_time", how="all", subset=["Sv"])

    return ds_sv
