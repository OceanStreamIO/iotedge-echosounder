"""Noise removal for edge Sv datasets.

Wraps ``oceanstream.echodata.denoise.apply_denoising`` which supports
four methods: background, transient, impulse, and attenuation signal
removal — all configurable via ``DenoiseConfig``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

import xarray as xr

if TYPE_CHECKING:
    from oceanstream.echodata.config import DenoiseConfig

logger = logging.getLogger("oceanstream")


def denoise(
    ds_sv: xr.Dataset,
    *,
    config: Optional["DenoiseConfig"] = None,
) -> xr.Dataset:
    """Apply multi-method denoising to an Sv dataset.

    Parameters
    ----------
    ds_sv
        Sv dataset (output of ``compute_sv``).
    config
        Oceanstream ``DenoiseConfig``.  If ``None``, defaults are used
        (all four methods: background, transient, impulse, attenuation).

    Returns
    -------
    xr.Dataset
        Denoised Sv dataset.
    """
    from oceanstream.echodata.denoise import apply_denoising

    methods = config.methods if config else None
    logger.info("Denoising (methods=%s)", methods or "default")

    ds_denoised = apply_denoising(
        ds_sv,
        methods=methods,
        config=config,
    )

    logger.info("Denoising complete")
    return ds_denoised
